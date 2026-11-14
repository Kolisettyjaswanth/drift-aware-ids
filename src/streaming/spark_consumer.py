"""Bounded local Kafka -> Spark schema-v1 transport acceptance."""
import argparse
import json
import math
import os
from pathlib import Path
import signal
import subprocess
import sys
import uuid

from src.streaming.validation import (SPARK_VERSION, CONNECTOR, source_options,
                                     validate_record, read_records, summarize)


def write_json(path, value):
    path = Path(path)
    temp = path.with_suffix('.tmp')
    temp.write_text(json.dumps(value, indent=2, allow_nan=False)+'\n', encoding='utf-8')
    temp.replace(path)


def merge_accounting(report, counts, *, partial=False):
    protected = {'errors', 'drift', 'inference', 'accounting_complete'}
    if partial:
        protected.add('success')
    report.update({key:value for key,value in counts.items() if key not in protected})
    if report.get('errors'):
        report['success'] = False


class BatchSink:
    """Persist validated batches and optionally their model predictions.

    Transport batch files remain the completion marker.
    This is a bounded local sink, not a production transaction system.
    """

    def __init__(self, run_dir, max_records, inference_engine=None, drift_monitor=None):
        self.directory = Path(run_dir) / "batches"
        self.directory.mkdir()
        self.max_records = max_records
        self.sizes = {}
        self.inference_engine = inference_engine
        self.drift_monitor = drift_monitor

        self.prediction_directory = None
        if inference_engine is not None:
            self.prediction_directory = Path(run_dir) / "predictions"
            self.prediction_directory.mkdir()

    def __call__(self, frame, batch_id):
        path = self.directory / f"batch-{batch_id:020d}.jsonl"
        temp = path.with_suffix(".tmp")

        prediction_path = None
        prediction_temp = None

        if self.prediction_directory is not None:
            prediction_path = (
                self.prediction_directory
                / f"batch-{batch_id:020d}.jsonl"
            )
            prediction_temp = prediction_path.with_suffix(".tmp")

        count = 0
        previous = sum(
            n for batch, n in self.sizes.items()
            if batch != batch_id
        )
        records = []

        try:
            with temp.open("w", encoding="utf-8") as stream:
                for row in frame.toLocalIterator(
                    prefetchPartitions=False
                ):
                    count += 1

                    if previous + count > self.max_records:
                        raise ValueError(
                            "Input safety cap exceeded; "
                            "batch accounting incomplete"
                        )

                    record = validate_record(row)

                    stream.write(
                        json.dumps(record, allow_nan=False) + "\n"
                    )

                    if self.inference_engine is not None:
                        records.append(record)

            if self.inference_engine is not None:
                from src.streaming.inference_batch import (
                    predict_validated_batch,
                )

                predictions = predict_validated_batch(
                    records,
                    engine=self.inference_engine,
                )

                with prediction_temp.open(
                    "w", encoding="utf-8"
                ) as stream:
                    for prediction in predictions:
                        stream.write(
                            json.dumps(
                                prediction,
                                allow_nan=False,
                            ) + "\n"
                        )

                prediction_temp.replace(prediction_path)

            temp.replace(path)
            self.sizes[batch_id] = count
            if self.drift_monitor is not None:
                self.drift_monitor.update() # after transport/prediction commit; never delete evidence

        finally:
            temp.unlink(missing_ok=True)

            if prediction_temp is not None:
                prediction_temp.unlink(missing_ok=True)


def execute_query(spark, config, drift_monitor=None):
    query = None
    try:
        options = source_options(config['brokers'], config['topic'], config['batch_size'])
        frame = spark.readStream.format('kafka').options(**options).load()
        inference_engine = None
        if config.get('enable_inference', False):
            from src.streaming.inference import StreamingInference
            inference_engine = StreamingInference()

        sink = BatchSink(
            config['run_dir'],
            config['max_records'],
            inference_engine=inference_engine,
            drift_monitor=drift_monitor,
        )
        query = (frame.select('key','value','topic','partition','offset')
                 .writeStream.foreachBatch(sink)
                 .option('checkpointLocation', str(Path(config['run_dir'])/'checkpoint'))
                 .trigger(availableNow=True).start())
        query.awaitTermination() # parent enforces wall-clock timeout, including startup
    finally:
        if query is not None and query.isActive:
            query.stop()


def worker(config):
    spark = None
    drift_monitor = None
    report = {'success': False, 'accounting_complete': False, 'errors': []}
    def interrupted(signum, frame):
        raise KeyboardInterrupt('Termination requested')
    old_handler = signal.signal(signal.SIGTERM, interrupted)
    try:
        if config.get('enable_drift', False):
            from src.streaming.drift_monitor import bound_reference, require_single_partition, DriftMonitor
            reference = bound_reference(config)
            require_single_partition(config['brokers'], config['topic'])
            drift_monitor = DriftMonitor(config, reference)
        import pyspark
        if pyspark.__version__ != SPARK_VERSION:
            raise ValueError(f'Expected PySpark {SPARK_VERSION}, got {pyspark.__version__}')
        from pyspark.sql import SparkSession
        spark = (SparkSession.builder.master('local[2]').appName('ids-kafka-acceptance')
                 .config('spark.jars.packages', CONNECTOR)
                 .config('spark.sql.shuffle.partitions','2')
                 .config('spark.default.parallelism','2')
                 .config('spark.ui.enabled','false')
                 .config('spark.sql.streaming.stopTimeout','10000')
                 .config('spark.driver.maxResultSize','64m')
                 .getOrCreate())
        if spark.version != SPARK_VERSION:
            raise ValueError(f'Unexpected JVM Spark version {spark.version}')
        scala = spark.sparkContext._jvm.scala.util.Properties.versionNumberString()
        if not scala.startswith('2.12.'):
            raise ValueError(f'Connector requires Scala 2.12, got {scala}')
        # Resolving the actual provider verifies the connector classpath at runtime.
        spark.sparkContext._jvm.java.lang.Class.forName(
            'org.apache.spark.sql.kafka010.KafkaSourceProvider', True,
            spark.sparkContext._jvm.java.lang.Thread.currentThread().getContextClassLoader())
        spark.sparkContext.setLogLevel('WARN')
        if drift_monitor is None:
            execute_query(spark, config)
        else:
            execute_query(spark, config, drift_monitor=drift_monitor)
        merge_accounting(report, summarize(read_records(config['run_dir']),config['replay_id'],
                                config['expected_count'],config['topic'],config['max_records']))
        if config.get('enable_inference', False):
            from src.streaming.verify_inference import verify_inference
            inference_result = verify_inference(config['run_dir'])
            report['inference'] = inference_result

            if not inference_result['verified']:
                raise ValueError("Inference verification failed")

        report.update(accounting_complete=True, spark_version=spark.version, scala_version=scala)
        if drift_monitor is not None:
            report['drift'] = drift_monitor.finalize()
            from src.streaming.drift_monitor import drift_succeeded
            report['success'] = report['success'] and drift_succeeded(report['drift'])
    except (Exception, KeyboardInterrupt) as exc:
        report['success'] = False
        report['errors'].append(str(exc)[:2000] or type(exc).__name__)
    finally:
        if spark is not None:
            try:
                spark.stop()
            except Exception as exc:
                report['success'] = False
                report['errors'].append('Spark cleanup failed: '+str(exc)[:500])
    if not report['accounting_complete']:
        try:
            merge_accounting(report, summarize(read_records(config['run_dir']), config['replay_id'],
                                    config['expected_count'], config['topic'], config['max_records']), partial=True)
        except Exception as exc:
            report['errors'].append('Partial accounting failed: '+str(exc)[:500])
        report['success'] = False
    if config.get('enable_drift', False):
        from src.streaming.drift_monitor import failed_summary, drift_succeeded
        report['drift'] = (drift_monitor.summary() if drift_monitor is not None else
                           failed_summary(config, '; '.join(report['errors']) or 'Drift initialization failed'))
        report['success'] = bool(report['success'] and drift_succeeded(report['drift']))
    report['success'] = bool(report['success'] and not report['errors'])
    signal.signal(signal.SIGTERM, old_handler)
    report.update(replay_id=config['replay_id'], expected_count=config['expected_count'],
                  topic=config['topic'], connector=CONNECTOR,
                  counts_scope='completed batch files only; incomplete on runtime failure')
    write_json(Path(config['run_dir'])/'summary.json', report)
    return 0 if report['success'] else 1


def terminate_group(process):
    """Linux/WSL process group includes Python, Spark JVM and workers."""
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        pass
    try:
        process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        pass
    finally:
        # Kill lingering children even if the Python worker exited first.
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass


def supervise(config, timeout):
    run_dir = Path(config['run_dir'])
    run_dir.mkdir(parents=True, exist_ok=False) # never reuse checkpoint or overwrite a run
    env = os.environ.copy()
    env['PYSPARK_SUBMIT_ARGS'] = '--driver-memory 1g pyspark-shell'
    env['PYSPARK_PYTHON'] = sys.executable
    process = None
    failure = None
    try:
        if config.get('enable_drift', False):
            from src.streaming.drift_monitor import prepare_drift_config
            config = prepare_drift_config(config)
        write_json(run_dir/'config.json',config)
        process = subprocess.Popen([sys.executable,'-m','src.streaming.spark_consumer',
                                    '--worker-config',str(run_dir/'config.json')],
                                   env=env, start_new_session=True)
        code = process.wait(timeout=timeout)
        if code:
            failure = f'Spark worker exited {code}'
    except subprocess.TimeoutExpired:
        failure = f'Wall-clock timeout after {timeout}s'
    except KeyboardInterrupt:
        failure = 'Interrupted by user'
    except Exception as exc:
        failure = str(exc)
    finally:
        if process is not None:
            terminate_group(process)
    path = run_dir/'summary.json'
    report = json.loads(path.read_text()) if path.exists() else {
        'success': False, 'accounting_complete': False, 'errors': []}
    if not path.exists():
        try:
            merge_accounting(report, summarize(read_records(run_dir), config['replay_id'],
                                config['expected_count'], config['topic'], config['max_records']), partial=True)
        except Exception as exc:
            report['errors'].append('Partial accounting failed: '+str(exc))
        report.update(success=False, accounting_complete=False,
                      counts_scope='completed batch files only; runtime did not finish')
    if failure:
        report['success'] = False
        report.setdefault('errors',[]).append(failure)
    if config.get('enable_drift', False) and (failure or 'drift' not in report):
        from src.streaming.drift_monitor import failed_summary
        if 'drift' not in report:
            report['drift'] = failed_summary(config, failure or 'Worker did not report drift')
        report['success'] = False
    if config.get('enable_drift', False):
        from src.streaming.drift_monitor import drift_succeeded
        report['success'] = bool(report['success'] and drift_succeeded(report.get('drift', {})))
    report['success'] = bool(report['success'] and not report.get('errors'))
    report['run_dir'] = str(run_dir)
    write_json(path,report)
    return report


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--worker-config', help=argparse.SUPPRESS)
    p.add_argument('--enable-inference', action='store_true')
    p.add_argument('--enable-drift', action='store_true')
    p.add_argument('--drift-reference')
    p.add_argument('--brokers',default='127.0.0.1:9092')
    p.add_argument('--topic')
    p.add_argument('--replay-id')
    p.add_argument('--expected-count',type=int,default=100)
    p.add_argument('--max-records',type=int,default=1000)
    p.add_argument('--batch-size',type=int,default=100)
    p.add_argument('--timeout',type=float,default=180.)
    p.add_argument('--run-dir',default=None)
    args = p.parse_args()
    if args.worker_config:
        return worker(json.loads(Path(args.worker_config).read_text()))
    try:
        if args.enable_drift and not args.drift_reference:
            raise ValueError('--drift-reference is required with --enable-drift')
        if args.drift_reference and not args.enable_drift:
            raise ValueError('--drift-reference requires --enable-drift')
        source_options(args.brokers,args.topic,args.batch_size)
        summarize([],args.replay_id,args.expected_count,args.topic,args.max_records)
        if not math.isfinite(args.timeout) or args.timeout <= 0:
            raise ValueError('Timeout must be finite and positive')
        config = {k: getattr(args,k) for k in ('brokers','topic','replay_id','expected_count',
                                              'max_records','batch_size')}
        config['enable_inference'] = args.enable_inference
        config['enable_drift'] = args.enable_drift
        config['drift_reference'] = args.drift_reference
        config['run_dir'] = str(Path(args.run_dir or
            f'data/processed/spark/{uuid.uuid4()}').resolve())
        report = supervise(config,args.timeout)
    except Exception as exc:
        report = {'success':False,'errors':[str(exc)]}
    print(json.dumps(report,allow_nan=False))
    return 0 if report['success'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
