import MySQLdb
import logging
import optparse
import os
import time
import warnings

LOGGING_DIR = '/var/log/percona/'

def logging_setup(log_name, logging_level):
    if not os.path.isdir(LOGGING_DIR):
        os.makedirs(LOGGING_DIR)

    mtk_logger = logging.getLogger('mysql_trx_killer')
    mtk_logger.setLevel(logging_level)
    formatter = logging.Formatter(
        '%(asctime)s %(process)d %(levelname)s-%(name)s::%(message)s',
        '%Y-%m-%d %H:%M:%S'
        )

    mtk_fh = logging.FileHandler(os.path.join(LOGGING_DIR, log_name))
    mtk_fh.setLevel(logging_level)
    mtk_fh.setFormatter(formatter)
    mtk_logger.addHandler(mtk_fh)

    mtk_sh = logging.StreamHandler()
    mtk_sh.setLevel(logging.WARNING)
    mtk_sh.setFormatter(formatter)
    mtk_logger.addHandler(mtk_sh)

def trx_guardian_setup(server_data):
    """Perform initial setup for trx_guardian process
    """

    mtk_logger = logging.getLogger('mysql_trx_killer')

    mydb = MySQLdb.connect(
        read_default_group='client',
        host=server_data['HOST'],
        port=int(server_data.get('PORT', 3306))
        )

    with mydb:

        cursor_admin = mydb.cursor(MySQLdb.cursors.DictCursor)
        warnings.filterwarnings("ignore",
                                "Table.*already exists")
        mtk_logger.debug('''creating table percona.trx_meta if it doesn't exist
                         ''')

        try:
            attempt = cursor_admin.execute("""
                CREATE TABLE IF NOT EXISTS percona.trx_meta (
                    `id` INT UNSIGNED NOT NULL AUTO_INCREMENT, 
                    `current_time` DATETIME NOT NULL, 
                    `transaction` VARCHAR(18) NOT NULL, 
                    `start_time` DATETIME NOT NULL, 
                    `active` INT UNSIGNED NOT NULL, 
                    `trx_state` VARCHAR(13) NOT NULL, 
                    `lock_structs` BIGINT UNSIGNED NOT NULL, 
                    `heap_size` BIGINT UNSIGNED NOT NULL, 
                    `row_locks` BIGINT UNSIGNED NOT NULL, 
                    `undo_log_entries` BIGINT UNSIGNED NOT NULL, 
                    `weight` BIGINT UNSIGNED NOT NULL, 
                    `tables_in_use` BIGINT UNSIGNED NOT NULL, 
                    `tables_locked` BIGINT UNSIGNED NOT NULL, 
                    `isolation` VARCHAR(16) NOT NULL, 
                    `concurrency_tickets` BIGINT UNSIGNED NOT NULL, 
                    `unique_checks` INT UNSIGNED NOT NULL, 
                    `fk_checks` INT UNSIGNED NOT NULL, 
                    `last_fk_error` VARCHAR(256) NULL, 
                    `adaptive_hash_latched` INT UNSIGNED NOT NULL, 
                    `adaptive_hash_timeout` BIGINT UNSIGNED NOT NULL, 
                    `pid` BIGINT UNSIGNED NOT NULL, 
                    `user` VARCHAR(16) NOT NULL, 
                    `host` VARCHAR(64) NOT NULL, 
                    `database` VARCHAR(64) NULL, 
                    `command` VARCHAR(16) NOT NULL, 
                    `time` BIGINT UNSIGNED NOT NULL, 
                    `state` VARCHAR(64) NULL, 
                    `query` LONGTEXT NULL, 
                    `rows_sent` BIGINT UNSIGNED NULL, 
                    `rows_examined` BIGINT UNSIGNED NULL, 
                    `requested_lock_id` VARCHAR(81) NULL, 
                    `r_lock_mode` VARCHAR(32) NULL, 
                    `r_lock_type` VARCHAR(32) NULL, 
                    `r_lock_table` VARCHAR(1024) NULL, 
                    `r_lock_data` VARCHAR(8192) NULL, 
                    `r_lock_wait_time` DATETIME NULL, 
                    `blocking_lock_id` VARCHAR(81) NULL, 
                    `blocking_transaction` VARCHAR(18) NULL, 
                    `b_start_time` DATETIME NULL, 
                    `b_active` INT UNSIGNED NULL, 
                    `b_trx_state` VARCHAR(13) NULL, 
                    `b_lock_structs` BIGINT UNSIGNED NULL, 
                    `b_heap_size` BIGINT UNSIGNED NULL, 
                    `b_row_locks` BIGINT UNSIGNED NULL, 
                    `b_undo_log_entries` BIGINT UNSIGNED NULL, 
                    `b_pid` BIGINT UNSIGNED NULL, 
                    `b_user` VARCHAR(16) NULL, 
                    `b_host` VARCHAR(64) NULL, 
                    `b_database` VARCHAR(64) NULL, 
                    `b_command` VARCHAR(16) NULL, 
                    `b_time` BIGINT UNSIGNED NULL, 
                    `b_state` VARCHAR(64) NULL, 
                    `b_query` LONGTEXT, 
                    `b_rows_sent` BIGINT UNSIGNED NULL, 
                    `b_rows_examined` BIGINT UNSIGNED NULL,
                    PRIMARY KEY(id))
                    ENGINE = MyISAM""")

        except mydb.Error as mysql_error:
            mtk_logger.exception(
                "create table percona.trx_meta failed - mysql error %d: %s",
                mysql_error.args[0], mysql_error.args[1])
            raise StandardError(
                "create table percona.trx_meta failed - mysql error %d: %s"
                % (mysql_error.args[0], mysql_error.args[1]))

def trx_guardian_meta(server_data, meta_time=30):
    """ Function to gather meta data and log to table
    meta_time:              When trx's have been identified and logged, log
                            metadata for transactions that have been running
                            longer than this threshold to the percona.trx_meta
                            table (seconds).
    """

    mydb = MySQLdb.connect(read_default_group='client',
                           host=server_data['HOST'],
                           port=int(server_data.get('PORT', 3306)))

    mtk_logger = logging.getLogger('mysql_trx_killer')

    with mydb:

        cursor_admin = mydb.cursor(MySQLdb.cursors.DictCursor)
        warnings.filterwarnings("ignore",
                                "Table.*already exists")
        mtk_logger.debug('''creating table percona.trx_meta if it doesn't exist
                         ''')

        try:
            cursor_admin.execute("""INSERT IGNORE INTO percona.trx_meta (
                                        `current_time`, 
                                        `transaction`, 
                                        `start_time`, 
                                        `active`, 
                                        `trx_state`, 
                                        `lock_structs`, 
                                        `heap_size`, 
                                        `row_locks`, 
                                        `undo_log_entries`, 
                                        `weight`, 
                                        `tables_in_use`, 
                                        `tables_locked`, 
                                        `isolation`, 
                                        `concurrency_tickets`, 
                                        `unique_checks`, 
                                        `fk_checks`, 
                                        `last_fk_error`, 
                                        `adaptive_hash_latched`, 
                                        `adaptive_hash_timeout`, 
                                        `pid`, 
                                        `user`, 
                                        `host`, 
                                        `database`, 
                                        `command`, 
                                        `time`,
                                        `state`, 
                                        `query`, 
                                        `rows_sent`, 
                                        `rows_examined`, 
                                        `requested_lock_id`, 
                                        `r_lock_mode`, 
                                        `r_lock_type`, 
                                        `r_lock_table`, 
                                        `r_lock_data`, 
                                        `r_lock_wait_time`, 
                                        `blocking_lock_id`, 
                                        `blocking_transaction`, 
                                        `b_start_time`, 
                                        `b_active`, 
                                        `b_trx_state`, 
                                        `b_lock_structs`, 
                                        `b_heap_size`, 
                                        `b_row_locks`, 
                                        `b_undo_log_entries`, 
                                        `b_pid`, 
                                        `b_user`, 
                                        `b_host`, 
                                        `b_database`, 
                                        `b_command`, 
                                        `b_time`,
                                        `b_state`, 
                                        `b_query`, 
                                        `b_rows_sent`, 
                                        `b_rows_examined`) 
                                    SELECT now() AS `current_time`, 
                                           trx.trx_id AS `transaction`, 
                                           trx.trx_started AS `start_time`, 
                                           (UNIX_TIMESTAMP() - UNIX_TIMESTAMP(trx.trx_started)) AS `active`, 
                                           trx.trx_state AS `state`, 
                                           trx.trx_lock_structs AS `lock_structs`, 
                                           trx.trx_lock_memory_bytes AS `heap_size`, 
                                           trx.trx_rows_locked AS `row_locks`, 
                                           trx.trx_rows_modified AS `undo_log_entries`, 
                                           trx.trx_weight AS `weight`, 
                                           trx.trx_tables_in_use AS `tables_in_use`, 
                                           trx.trx_tables_locked AS `tables_locked`, 
                                           trx.trx_isolation_level AS `isolation`, 
                                           trx.trx_concurrency_tickets AS `concurrency_tickets`, 
                                           trx.trx_unique_checks AS `unique_checks`, 
                                           trx.trx_foreign_key_checks AS `fk_checks`, 
                                           trx.trx_last_foreign_key_error AS `last_fk_error`, 
                                           trx.trx_adaptive_hash_latched AS `adaptive_hash_latched`, 
                                           trx.trx_adaptive_hash_timeout AS `adaptive_hash_timeout`, 
                                           trx.trx_mysql_thread_id AS `pid`, ps.user AS `user`, 
                                           substring_index(ps.host,':',1) AS `host`, 
                                           ps.db AS `database`, 
                                           ps.command AS `command`, 
                                           ps.time AS `time`,
                                           ps.state AS `state`, 
                                           trx.trx_query AS `query`, 
                                           ps.rows_sent AS `rows_sent`, 
                                           ps.rows_examined AS `rows_examined`,
                                           trx.trx_requested_lock_id AS `requested_lock_id`, 
                                           rlk.lock_mode AS `r_lock_mode`, 
                                           rlk.lock_type AS `r_lock_type`, 
                                           rlk.lock_table AS `r_lock_table`, 
                                           rlk.lock_data AS `r_lock_data`, 
                                           trx.trx_wait_started AS `r_lock_wait_time`, 
                                           lw.blocking_lock_id AS `blocking_lock_id`, 
                                           lw.blocking_trx_id AS `blocking_transaction`, 
                                           blk.trx_started AS `b_start_time`, 
                                           (UNIX_TIMESTAMP() - UNIX_TIMESTAMP(blk.trx_started)) AS `b_active`, 
                                           blk.trx_state AS `b_state`, 
                                           blk.trx_lock_structs AS `b_lock_structs`, 
                                           blk.trx_lock_memory_bytes AS `b_heap_size`, 
                                           blk.trx_rows_locked AS `b_row_locks`, 
                                           blk.trx_rows_modified AS `b_undo_log_entries`, 
                                           blk.trx_mysql_thread_id AS `b_pid`, 
                                           ps2.user AS `b_user`, 
                                           substring_index(ps2.host,':',1) AS `b_host`, 
                                           ps2.db AS `b_database`, 
                                           ps2.command AS `b_command`, 
                                           ps2.time AS `b_time`,
                                           ps2.state AS `b_state`, 
                                           blk.trx_query AS `b_query`, 
                                           ps2.rows_sent AS `b_rows_sent`, 
                                           ps2.rows_examined AS `b_rows_examined`
                                    FROM INFORMATION_SCHEMA.INNODB_TRX trx  
                                    JOIN INFORMATION_SCHEMA.PROCESSLIST ps ON trx.trx_mysql_thread_id = ps.id 
                                    LEFT JOIN INFORMATION_SCHEMA.INNODB_LOCKS rlk ON trx.trx_requested_lock_id = rlk.lock_id 
                                    LEFT JOIN INFORMATION_SCHEMA.INNODB_LOCK_WAITS lw ON trx.trx_id = lw.requesting_trx_id 
                                    LEFT JOIN INFORMATION_SCHEMA.INNODB_TRX blk ON lw.blocking_trx_id = blk.trx_id 
                                    LEFT JOIN INFORMATION_SCHEMA.PROCESSLIST ps2 ON blk.trx_mysql_thread_id = ps2.id 
                                    WHERE ((UNIX_TIMESTAMP() - UNIX_TIMESTAMP(trx.trx_started)) >= %d OR trx.trx_requested_lock_id IS NOT NULL) 
                                    ORDER BY (UNIX_TIMESTAMP() - UNIX_TIMESTAMP(trx.trx_started))"""
                                 % (meta_time))
        except mydb.Error as mysql_error:
            mtk_logger.exception(
                "couldn't insert current trx metadata - mysql error %d: %s",
                mysql_error.args[0], mysql_error.args[1])
            raise StandardError(
                "couldn't insert current trx metadata - mysql error %d: %s"
                % (mysql_error.args[0], mysql_error.args[1]))

def trx_guardian(server_data,
                 do_kill_trx=False,
                 sleep_time=0,
                 blocking_threshold=30,
                 sleep_threshold=120,
                 long_running_threshold=300,
                 capture_meta=False):

    """Process to kill long running transactions

    server_data:            Python dict containing host and port details
                            e.g. {'HOST':127.0.0.1,'PORT':3306}.
    do_kill_trx:            If true, kill the transactions identified,
                            excluding those that have made modifications.
    do_kill_undo_trx:       If true, do additionally kill the transactions
                            identified that have made modifications.
    sleep_time:             Seconds to sleep before looking for blocking
                            processes.
    blocking_threshold:     Identify and log transactions that have been
                            blocking other transactions for at least this
                            amount of time (seconds).
    sleep_threshold:        Identify and log transactions that are currently
                            in sleep, and have been for at least this amount
                            of time (seconds).
    long_running_threshold: Identify and log transactions that have been running
                            for at least this amount of time (seconds).

    """

    mtk_logger = logging.getLogger('mysql_trx_killer')
    time.sleep(sleep_time)  # arbitrary sleep time, wait and see

    mtk_logger.debug('mysql_trx_killer has started')
    mydb = MySQLdb.connect(read_default_group='client',
                           host=server_data['HOST'],
                           port=int(server_data.get('PORT', 3306)))
    mtk_logger.debug('mysql connection created')

    with mydb:

        cursor_trxs = mydb.cursor(MySQLdb.cursors.DictCursor)
        cursor_admin = mydb.cursor(MySQLdb.cursors.DictCursor)

        mtk_logger.debug('checking for killable trxs')

        try:
            cursor_trxs.execute("""SELECT tbl.pid,
                                          tbl.transaction, 
                                          tbl.user,
                                          tbl.host,
                                          tbl.database,
                                          tbl.command, 
                                          tbl.state,
                                          tbl.info,
                                          MAX(tbl.active) AS `active`, 
                                          MAX(tbl.undo_log_entries) AS `undo_log_entries`, 
                                          MAX(tbl.time) AS `time`,
                                          GROUP_CONCAT(DISTINCT tbl.reason) AS `reason`
                                   FROM (SELECT blk.trx_mysql_thread_id AS `pid`, 
                                                blk.trx_id AS `transaction`, 
                                                (UNIX_TIMESTAMP() - UNIX_TIMESTAMP(blk.trx_started)) AS `active`, 
                                                blk.trx_rows_modified AS `undo_log_entries`, 
                                                ps2.user AS `user`,
                                                substring_index(ps2.host,':',1) AS `host`, 
                                                ps2.db AS `database`, 
                                                ps2.command AS `command`, 
                                                ps2.time AS `time`, 
                                                ps2.state AS `state`, 
                                                ps2.info AS `info`,
                                                'BLOCKING' AS `reason`          
                                         FROM (SELECT lw1.blocking_trx_id AS `blocking_trx_id`,
                                               MAX((UNIX_TIMESTAMP() - UNIX_TIMESTAMP(trx1.trx_wait_started))) AS `blocking_for` 
                                               FROM INFORMATION_SCHEMA.INNODB_TRX trx1 
                                               JOIN INFORMATION_SCHEMA.INNODB_LOCK_WAITS lw1 ON lw1.requesting_trx_id = trx1.trx_id
                                               GROUP BY lw1.blocking_trx_id
                                               HAVING blocking_for >= %s) lw 
                                         JOIN INFORMATION_SCHEMA.INNODB_TRX blk ON lw.blocking_trx_id = blk.trx_id 
                                         JOIN INFORMATION_SCHEMA.PROCESSLIST ps2 ON blk.trx_mysql_thread_id = ps2.id
                                         WHERE ps2.user != 'system_user' 
                                         UNION 
                                         SELECT trx.trx_mysql_thread_id AS `pid`, 
                                                trx.trx_id AS `transaction`, 
                                                (UNIX_TIMESTAMP() - UNIX_TIMESTAMP(trx.trx_started)) AS `active`, 
                                                trx.trx_rows_modified AS `undo_log_entries`, 
                                                ps.user AS `user`,
                                                substring_index(ps.host,':',1) AS `host`, 
                                                ps.db AS `database`, 
                                                ps.command AS `command`, 
                                                ps.time AS `time`, 
                                                ps.state AS `state`, 
                                                ps.info AS `info`, 
                                                'LONG_SLEEP' AS `reason` 
                                         FROM INFORMATION_SCHEMA.INNODB_TRX trx 
                                         JOIN INFORMATION_SCHEMA.PROCESSLIST ps ON trx.trx_mysql_thread_id = ps.id  
                                         WHERE ps.time >= %s AND ps.command = 'Sleep' AND ps.user != 'system_user'
                                         UNION
                                         SELECT trx.trx_mysql_thread_id AS `pid`, 
                                                trx.trx_id AS `transaction`, 
                                                (UNIX_TIMESTAMP() - UNIX_TIMESTAMP(trx.trx_started)) AS `active`, 
                                                trx.trx_rows_modified AS `undo_log_entries`, 
                                                ps.user AS `user`,
                                                substring_index(ps.host,':',1) AS `host`, 
                                                ps.db AS `database`, 
                                                ps.command AS `command`, 
                                                ps.time AS `time`, 
                                                ps.state AS `state`, 
                                                ps.info AS `info`, 
                                                'LONG_TRX' AS `reason` 
                                         FROM INFORMATION_SCHEMA.INNODB_TRX trx 
                                         JOIN INFORMATION_SCHEMA.PROCESSLIST ps ON trx.trx_mysql_thread_id = ps.id  
                                         WHERE (UNIX_TIMESTAMP() - UNIX_TIMESTAMP(trx.trx_started)) >= %s AND ps.user != 'system_user') tbl
                                   GROUP BY tbl.pid, tbl.transaction, tbl.user, tbl.host, tbl.database, tbl.command, tbl.state, tbl.info
                                   ORDER BY tbl.active"""
                                % (blocking_threshold,
                                   sleep_threshold,
                                   long_running_threshold))
        except mydb.Error as mysql_error:
            mtk_logger.exception(
                "couldn't extract list of trxs to kill - mysql error %d: %s",
                mysql_error.args[0], mysql_error.args[1])
            raise StandardError(
                "couldn't extract list of trxs to kill - mysql error %d: %s"
                % (mysql_error.args[0], mysql_error.args[1]))

        if cursor_trxs.rowcount > 0:
            mtk_logger.debug('trxs found, capturing trx metadata')
            if capture_meta:
                trx_guardian_meta(server_data, capture_meta)

            for row in cursor_trxs:
                row = cursor_trxs.fetchone()

                logging_data = {
                    'PID':'%d' % (row["pid"]),
                    'Transaction':row["transaction"],
                    'ACTIVE':'%d' % (row["active"]),
                    'Undo Log Entries':'%d' % row["undo_log_entries"],
                    'User':row["user"],
                    'Host':row["host"],
                    'Schema':row["database"],
                    'Command':row["command"],
                    'Time':'%d' % (row["time"]),
                    'State':row["state"],
                    'Info':row["info"],
                    'Reason':row["reason"]
                }

                mtk_logger.info('killing trx ---> %s', logging_data)

                if do_kill_trx == 1:
                    if row["undo_log_entries"] == 0:
                        try:
                            cursor_admin.execute("kill %s", row["pid"])
                        except mydb.Error as mysql_error:
                            mtk_logger.exception(
                                "failed to kill trx - mysql error %d: %s",
                                mysql_error.args[0], mysql_error.args[1])
                            raise StandardError(
                                "failed to kill trx - mysql error %d: %s"
                                % (mysql_error.args[0], mysql_error.args[1]))
                    else:
                        mtk_logger.info('kill skipped due to associated undo log entries')
                elif do_kill_trx == 2:
                    try:
                        cursor_admin.execute("kill %s", row["pid"])
                    except mydb.Error as mysql_error:
                        mtk_logger.exception(
                            "failed to kill trx - mysql error %d: %s",
                            mysql_error.args[0], mysql_error.args[1])
                        raise StandardError(
                            "failed to kill trx - mysql error %d: %s"
                            % (mysql_error.args[0], mysql_error.args[1]))
                else:
                    mtk_logger.info('kill disabled, skipping kill pid')
        else:
            mtk_logger.debug('found no trxs to kill')

    mtk_logger.debug('mysql_trx_killer has ended')
    return

def main():
    """Main method, loads in args and executes trx_guardian
    """

    parser = optparse.OptionParser()
    parser.add_option("-s", "--host",
                      dest='host',
                      default="localhost",
                      help="Host ip or fqdn [default: %default]")
    parser.add_option("-p", "--port",
                      dest='port',
                      default=3306,
                      help="MySQL Port [default: %default]")
    parser.add_option("-b", "--blocktime",
                      dest='block_threshold',
                      default=30,
                      help="blocking transaction threshold [default: %default]")
    parser.add_option("-z", "--sleeptime",
                      dest='sleep_threshold',
                      default=120,
                      help="sleeping transaction threshold [default: %default]")
    parser.add_option("-l", "--longtime",
                      dest='long_threshold',
                      default=300,
                      help="long running transaction threshold [default: %default]")
    parser.add_option("-t", dest='killtrx',
                      default=False,
                      action='store_true',
                      help="kill the ro transactions identified [default: %default]")
    parser.add_option("-u", dest='killundo',
                      default=False,
                      action='store_true',
                      help="additionally kill the rw transactions identified [default: %default]")
    parser.add_option("-m", dest='meta',
                      default=30,
                      action='store_true',
                      help="threshold to capture metadata to table [default: %default]")
    parser.add_option("-x", dest='daemon',
                      default=False,
                      action='store_true',
                      help="run continuously [default: %default]")
    parser.add_option("-i", "--interval",
                      dest='interval',
                      type="int",
                      default=60,
                      help="interval between invocations when running in daemon mode [default: %default]")
    parser.add_option("-d", dest='debug',
                      default=False,
                      action='store_true',
                      help="log debug messages [default: %default]")

    opts, _ = parser.parse_args()
    server_data = {'HOST':opts.host, 'PORT':opts.port}
    if opts.debug:
        logging_level = logging.DEBUG
    else:
        logging_level = logging.INFO

    logging_setup('mysql_trx_killer.log', logging_level)
    sleep_time = opts.interval

    if opts.meta:
        trx_guardian_setup(server_data)

    if opts.killtrx:
        do_kill = 1
    elif opts.killundo:
        do_kill = 2
    else:
        do_kill = False

    if opts.daemon:
        while 1:
            trx_guardian(server_data,
                         do_kill_trx=do_kill,
                         sleep_time=sleep_time,
                         blocking_threshold=opts.block_threshold,
                         sleep_threshold=opts.sleep_threshold,
                         long_running_threshold=opts.long_threshold,
                         capture_meta=opts.meta)
    else:
        trx_guardian(server_data,
                     do_kill_trx=do_kill,
                     blocking_threshold=opts.block_threshold,
                     sleep_threshold=opts.sleep_threshold,
                     long_running_threshold=opts.long_threshold,
                     capture_meta=opts.meta)

if __name__ == '__main__':
    main()
