"""
Test suite that verifies the deployment of Ceph NVMeoF Gateway HA
 with supported entities like subsystems , etc.,

"""

import json
import time
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy

from looseversion import LooseVersion

from ceph.ceph import Ceph
from ceph.ceph_admin.orch import Orch
from ceph.parallel import parallel
from tests.nvmeof.workflows.gateway_entities import (
    configure_hosts,
    configure_listeners,
    configure_namespaces,
    configure_subsystems,
    fetch_namespaces,
    teardown,
)
from tests.nvmeof.workflows.initiator import (
    compare_client_namespace,
    prepare_io_execution,
)
from tests.nvmeof.workflows.load_balancing import (
    scale_down,
    scale_up,
    validate_auto_loadbalance,
    validate_scaleup,
)
from tests.nvmeof.workflows.nvme_service import NVMeService
from tests.nvmeof.workflows.nvme_utils import (
    check_and_set_nvme_cli_image,
    check_gateway,
)
from tests.rbd.rbd_utils import initial_rbd_config
from utility.log import Log
from utility.utils import (
    generate_unique_id,
    get_ceph_version_from_cluster,
)

LOG = Log(__name__)


def fetch_lb_groups(nvme_service, nodes):
    """Fetch Load balancing group ids for given nodes."""
    lb_group_ids = {}
    for node in nodes:
        nvmegwcli = check_gateway(nvme_service.gateways, node)
        hostname = nvmegwcli.fetch_gateway_hostname()
        lb_group_ids.update({hostname: nvmegwcli.ana_group_id})
    return lb_group_ids


def delete_namespaces(nvmegwcli, ns_count, subsystem):
    with parallel() as p:
        for num in range(1, ns_count + 1):
            ns_args = {
                "args": {
                    "nsid": num,
                    "subsystem": subsystem,
                }
            }
            p.spawn(nvmegwcli.namespace.delete, **ns_args)


def parse_namespaces(config, namespaces):
    all_namespaces = []
    for subsystem in config["subsystems"]:
        sub_name = subsystem["nqn"]
        for ns in namespaces:
            # <subsystem>|<nsid>|<pool_name>|<image>
            ns_info = f"nsid-{ns['nsid']}|{ns['rbd_pool_name']}|{ns['rbd_image_name']}"
            all_namespaces.append(f"{sub_name}|{ns_info}")
    return all_namespaces


def test_ceph_83608838(ceph_cluster, config):
    rbd_pool = config["rbd_pool"]
    rbd_obj = config["rbd_obj"]
    # Deploy nvmeof service
    LOG.info("deploy nvme service")
    deploy_nvme_service(ceph_cluster, config)
    ha = HighAvailability(ceph_cluster, config["gw_nodes"], **config)

    # Configure subsystems
    LOG.info("Configure subsystems")
    with parallel() as p:
        for subsys_args in config["subsystems"]:
            subsys_args["ceph_cluster"] = ceph_cluster
            p.spawn(configure_subsystems, rbd_pool, ha, subsys_args)

    # Configure namespaces
    LOG.info("Configure namespaces")
    for subsystem in config["subsystems"]:
        for image_num in range(1, 11):
            sub_name = subsystem["nqn"]
            nvmegwcl = ha.gateways[0]
            image = f"image-{generate_unique_id(6)}-{image_num}"
            rbd_obj.create_image(rbd_pool, image, "1G")
            img_args = {
                "subsystem": f"{sub_name}",
                "rbd-pool": rbd_pool,
                "rbd-image": image,
                "load-balancing-group": 4,
            }
            nvmegwcl.namespace.add(**{"args": {**img_args}})

    # wait for 180 seconds and check for autoload balancing
    LOG.info("wait for 180 seconds and check for autoload balancing")
    time.sleep(180)
    ha.validate_auto_loadbalance()

    # Delete namespaces related to one load balancing group in each subsysyem
    LOG.info("Delete namespaces related to one load balancing group in each subsysyem")
    for subsystem in config["subsystems"]:
        sub_name = subsystem["nqn"]
        img_args = {"subsystem": f"{sub_name}"}
        namespace_list = nvmegwcl.namespace.list(
            **{"args": {**img_args}, "base_cmd_args": {"format": "json"}}
        )
        # Get the nsids related each load-balancing-group
        parsed_data = json.loads(namespace_list[1])
        grouped_nsids = dict()
        for ns in parsed_data["namespaces"]:
            group = ns["load_balancing_group"]
            nsid = ns["nsid"]
            if group not in grouped_nsids:
                grouped_nsids[group] = list()
            grouped_nsids[group].append(nsid)
        nsids_to_delete = grouped_nsids[4]
        for nsid in nsids_to_delete:
            img_args = {"subsystem": f"{sub_name}", "nsid": f"{nsid}"}
            nvmegwcl.namespace.delete(**{"args": {**img_args}})

    # wait for 180 seconds and check for autoload balancing
    LOG.info("wait for 180 seconds and check for autoload balancing")
    time.sleep(180)
    ha.validate_auto_loadbalance()

    LOG.info(
        "CEPH-83608838 - Test load balancing for namespace addition and deletion \
             with same LB group test validated successfully."
    )


testcases = {
    "CEPH-83608838": test_ceph_83608838,
}


def run(ceph_cluster: Ceph, **kwargs) -> int:
    """Return the status of the Ceph NVMEof Load balancing test execution.

    - Configure Gateways
    - Configures Initiators and Run FIO on NVMe targets.
    - Perform scaleup and scaledown.
    - Validate the IO continuation prior and after to scaleup and scaledown

    Args:
        ceph_cluster: Ceph cluster object
        kwargs: Key/value pairs of configuration information to be used in the test.

    Returns:
        int - 0 when the execution is successful else 1 (for failure).

    Example:

        # Execute the nvmeof GW test
            - test:
                name: Ceph NVMeoF deployment
                desc: Configure NVMEoF gateways and initiators
                config:
                    gw_nodes:
                     - node6
                    rbd_pool: rbd
                    do_not_create_image: true
                    rep-pool-only: true
                    cleanup-only: true                          # only for cleanup
                    rep_pool_config:
                      pool: rbd
                    install: true                               # Run SPDK with all pre-requisites
                    subsystems:                                 # Configure subsystems with all sub-entities
                      - nqn: nqn.2016-06.io.spdk:cnode3
                        serial: 3
                        bdevs:
                          count: 1
                          size: 100G
                        listener_port: 5002
                        allow_host: "*"
                    initiators:                             # Configure Initiators with all pre-req
                      - nqn: connect-all
                        listener_port: 4420
                        node: node10
                    load_balancing:
                        - scale_down: ["node6", "node7"]             # scale down
                        - scale_up: ["node6", "node7"]               # scale up
                        - scale_up: ["node10", "node11"]               # new nodes scale up
    """
    LOG.info("Starting Ceph Ceph NVMEoF deployment.")
    config = kwargs["config"]
    rbd_pool = config["rbd_pool"]
    rbd_obj = initial_rbd_config(**kwargs)["rbd_reppool"]
    initiators = config.get("initiators")
    io_tasks = []
    executor = ThreadPoolExecutor()

    overrides = kwargs.get("test_data", {}).get("custom-config")
    check_and_set_nvme_cli_image(ceph_cluster, config=overrides)
    nvme_service = NVMeService(config, ceph_cluster)

    try:
        if config.get("test_case"):
            kwargs["config"].update(
                {
                    "do_not_create_image": True,
                    "rep-pool-only": True,
                    "rep_pool_config": {"pool": rbd_pool},
                }
            )
            rbd_obj = initial_rbd_config(**kwargs)["rbd_reppool"]
            test_case_run = testcases[config["test_case"]]
            config.update({"rbd_obj": rbd_obj})
            test_case_run(ceph_cluster, config)
        else:
            # Deploy NVMe services
            if config.get("install"):
                deploy_nvme_service(ceph_cluster, config)

            ha = HighAvailability(ceph_cluster, config["gw_nodes"], **config)
            gw_nodes = ha.gateways

            # Configure Subsystem
            if config.get("subsystems"):
                with parallel() as p:
                    for subsys_args in config["subsystems"]:
                        subsys_args["ceph_cluster"] = ceph_cluster
                        p.spawn(configure_subsystems, rbd_pool, ha, subsys_args)
                if ceph_cluster.rhcs_version > "8.0":
                    time.sleep(120)
                    ha.validate_auto_loadbalance()

            # Initiate scale-down and scale-up
            if config.get("load_balancing"):
                for lb_config in config.get("load_balancing"):
                    # namespace addition
                    if lb_config.get("ns_add"):
                        config = lb_config["ns_add"]
                        subsystems = config["subsystems"]
                        for subsystem in subsystems:
                            sub_args = {"subsystem": subsystem["nqn"]}
                            lb_groups = None
                            LOG.info(sub_args)
                            configure_namespaces(
                                ha.gateways[0],
                                subsystem,
                                lb_groups,
                                sub_args,
                                rbd_pool,
                                ceph_cluster,
                            )
                        if ceph_cluster.rhcs_version > "8.0":
                            ha.validate_auto_loadbalance()

                    # namespace deletion
                    if lb_config.get("ns_del"):
                        ns_del_config = lb_config["ns_del"]
                        LOG.info(ns_del_config)
                        ns_del_count = ns_del_config["count"]
                        subsystems = ns_del_config["subsystems"]
                        for subsystem in subsystems:
                            delete_namespaces(ha.gateways[0], ns_del_count, subsystem)
                        if ceph_cluster.rhcs_version > "8.0":
                            time.sleep(120)
                            ha.validate_auto_loadbalance()

                    # Scale down
                    if lb_config.get("scale_down"):
                        gateway_nodes_to_be_deployed = lb_config["scale_down"]
                        LOG.info(f"Started scaling down {gateway_nodes_to_be_deployed}")

                        # Prepare FIO Execution
                        namespaces = ha.fetch_namespaces(ha.gateways[0])
                        ha.prepare_io_execution(initiators)

                        # Check for targets at clients
                        ha.compare_client_namespace([i["uuid"] for i in namespaces])

                        # Start IO Execution
                        LOG.info("Initiating IO before scale down")
                        for initiator in ha.clients:
                            io_tasks.append(executor.submit(initiator.start_fio))
                        time.sleep(20)  # time sleep for IO to Kick-in

                        ha.scale_down(gateway_nodes_to_be_deployed)

                    # Scale up
                    if lb_config.get("scale_up"):
                        scaleup_nodes = lb_config["scale_up"]
                        gateway_nodes = config["gw_nodes"]
                        LOG.info(f"Started scaling up {scaleup_nodes}")

                        # Prepare FIO execution for existing namespaces
                        old_namespaces = ha.fetch_namespaces(ha.gateways[0])
                        ha.prepare_io_execution(initiators)

                        # Start IO Execution into already existing namespaces/nodes
                        LOG.info("Initiating IO before scale up ")
                        for initiator in ha.clients:
                            io_tasks.append(executor.submit(initiator.start_fio))
                        time.sleep(20)  # time sleep for IO to Kick-in

                        # Perform scale-up of new nodes
                        if not all(
                            [node in set(gateway_nodes) for node in scaleup_nodes]
                        ):
                            # Perform scale up
                            old_namespaces = parse_namespaces(config, old_namespaces)
                            ha.scale_up(scaleup_nodes, gw_nodes, old_namespaces)

                            # Add listeners and namespaces to newly added GWs
                            LOG.info(f"Adding listeners for {scaleup_nodes}")
                            for subsys_args in config["subsystems"]:
                                sub_args = {"subsystem": subsys_args["nqn"]}
                                lb_groups = configure_listeners(
                                    ha, scaleup_nodes, subsys_args
                                )

                            # Create new namespaces to newly added GWs that will take ANA_GRP of new GWs
                            LOG.info(f"Adding namespaces for {scaleup_nodes}")
                            for subsys_args in config["subsystems"]:
                                sub_args = {"subsystem": subsys_args["nqn"]}
                                configure_namespaces(
                                    ha.gateways[-1],
                                    subsys_args,
                                    lb_groups,
                                    sub_args,
                                    rbd_pool,
                                    ceph_cluster,
                                )

                            # Prepare FIO Execution for new namespaces
                            ha.prepare_io_execution(initiators)
                            new_namespaces = ha.fetch_namespaces(ha.gateways[-1])

                            # Check for targets at clients for new namespaces
                            ha.compare_client_namespace(
                                [i["uuid"] for i in new_namespaces]
                            )

                            # Start IO Execution for new namespaces
                            for initiator in ha.clients:
                                io_tasks.append(executor.submit(initiator.start_fio))
                            time.sleep(20)

                            # Validate IO for old namespaces
                            LOG.info("Validating IO for old namespaces post scaleup")
                            ha.validate_scaleup(scaleup_nodes, old_namespaces)

                            # Validate IO for new namespaces
                            LOG.info("Validating IO for new namespaces post scaleup")
                            namespaces = parse_namespaces(config, new_namespaces)
                            ha.validate_scaleup(scaleup_nodes, namespaces)

                        # Perform scale-up of old GW nodes(replacement)
                        else:
                            old_namespaces = parse_namespaces(config, old_namespaces)
                            ha.scale_up(scaleup_nodes, gw_nodes, old_namespaces)
                            ha.validate_scaleup(scaleup_nodes, old_namespaces)
        return 0

    except Exception as err:
        LOG.error(err)
        return 1

    finally:
        if config.get("cleanup"):
            teardown(nvme_service, rbd_obj)
