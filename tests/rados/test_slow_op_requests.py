import datetime
import re
import time
import traceback

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from tests.rados.rados_test_util import create_pools, get_slow_requests_log
from utility.log import Log
from utility.utils import method_should_succeed, should_not_be_empty

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Verify slow op requests
    Steps:
    1. Set the parameter osd_op_complaint_time to 0.01 at Global level
    2. Initialize rados bench write in the background
    3. Sleep for 10 secs and initialize rados bench random read in the background
    4. Observe ceph health detail for 240 secs, there should be reports of Slow ops
    """
    log.info(run.__doc__)
    config = kw["config"]
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    mon_obj = MonConfigMethods(rados_obj=rados_obj)
    installer = ceph_cluster.get_nodes(role="installer")[0]
    start_time = get_cluster_timestamp(rados_obj.node)
    log.debug(f"Test workflow started. Start time: {start_time}")
    try:
        log.info("Running slow op requests tests")
        _pool_name = config.pop("pool_name", "slow-op-pool")
        assert rados_obj.create_pool(pool_name=_pool_name, **config)

        mon_obj.set_config(
            section="global", name="osd_op_complaint_time", value="0.01"
        ), "osd_op_complaint_time could not be set correctly"

        start_time, err = installer.exec_command(
            cmd="sudo date -u '+%Y-%m-%d %H:%M:%S'"
        )
        osd_nodes = ceph_cluster.get_nodes(role="osd")

        pool = create_pools(config, rados_obj, client_node)
        should_not_be_empty(pool, "Failed to retrieve pool details")
        pools = config.get("create_pools")
        for each_pool in pools:
            cr_pool = each_pool["create_pool"]
            osd_services = rados_obj.list_orch_services(service_type="osd")
            for service in osd_services:
                client_node.exec_command(cmd=f"ceph orch restart {service}", sudo=True)
            for node in osd_nodes:
                log.info(f"Rebooting node : {node.hostname}")
                node.exec_command(sudo=True, cmd="reboot", check_ec=False)
            method_should_succeed(rados_obj.bench_write, **cr_pool)
        rados_obj.change_recovery_threads(config=pool, action="set")

        end_time, err = installer.exec_command(cmd="sudo date -u '+%Y-%m-%d %H:%M:%S'")
        if not slow_ops_frequency:
            log.error(
                "Slow Ops warning was NOT generated/captured during the 240 secs smart wait"
            )
            raise Exception(
                "Slow Ops warning was NOT generated/captured during the 120 secs smart wait"
            )

        log.info("Slow Ops request frequency: %s" % slow_ops_frequency)
        max_osd = max(slow_ops_frequency, key=slow_ops_frequency.get)
        log.info("OSD with highest count of Slow Ops: %s" % max_osd)

        init_logs = rados_obj.get_journalctl_log(
            start_time=start_time,
            end_time=end_time,
            daemon_type="osd",
            daemon_id=max_osd.split(".")[-1],
        )
        slow_ops_log_list = []
        for line in init_logs.splitlines():
            if "slow ops" in line:
                slow_ops_log_list.append(line)
                # check slow op count
                count = re.search(r"\b(\d+)\s+slow ops\b", line)
                if not count:
                    log.error("Slow Ops count not found in log line: " + line)
                    raise Exception("Slow Ops count not found in log line: " + line)
                log.info("Slow Ops count: %s" % count.group(1))

        if not slow_ops_log_list:
            log.error("No 'Slow Ops' entry found in the OSD log for OSD." + max_osd)
            raise Exception(
                "No 'Slow Ops' entry found in the OSD log for OSD." + max_osd
            )

        slow_op_logs = "\n".join(slow_ops_log_list)

        log.info(
            "\n ==========================================================================="
            "\n SLOW OPS entries in %s log: \n %s"
            "\n ==========================================================================="
            % (max_osd, slow_op_logs)
        )

        log.info("Generation of Slow Ops request warning has been verified")
    except Exception as e:
        log.info(e)
        log.error(traceback.format_exc())
        # log cluster health
        rados_obj.log_cluster_health()
        return 1
    finally:
        log.info(
            "\n \n ************** Execution of finally block begins here *************** \n \n"
        )
        # remove "osd_op_complaint_time" parameter
        mon_obj.remove_config(section="global", name="osd_op_complaint_time")
        # removal of rados pools
        rados_obj.rados_pool_cleanup()

        # log cluster health
        rados_obj.log_cluster_health()
        # check for crashes after test execution
        test_end_time = get_cluster_timestamp(rados_obj.node)
        log.debug(
            f"Test workflow completed. Start time: {start_time}, End time: {test_end_time}"
        )
        if rados_obj.check_crash_status(start_time=start_time, end_time=test_end_time):
            log.error("Test failed due to crash at the end of test")
            return 1
    return 0
