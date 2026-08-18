from base_consumer.BaseConsumer import BaseConsumer
from confluent_kafka import Consumer,KafkaException
from concurrent.futures import ThreadPoolExecutor,as_completed
from collections import defaultdict
from utils.maple_api_requests import MapleApiRequest
import json
import time


#airflow "publish_maple_api_request_dag"에서 publishing한 파라미터를 consume

class AirflowKafkaConsumer(BaseConsumer):
    def __init__(self, group_id):
        super().__init__(group_id)
        self.topics=['maple_character_api_param']

        conf = {
            'bootstrap.servers': self.BOOTSTRAP_SERVERS,
            'group.id': self.group_id,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': 'false'
        }

        self.consumer = Consumer(conf)
        self.consumer.subscribe(self.topics, on_assign=self.callback_on_assign)

    #consumer poll message
    def poll(self):
        try:

            empty_since = None

            while True:
                msg_lst = self.consumer.consume(num_messages=5,timeout=1.0)
                if not msg_lst:
                    if empty_since is None:
                        empty_since = time.time()

                    if time.time() - empty_since >= 10:
                        self.logger.info('No messages for 10 seconds. Consumer stopped.')
                        break

                    continue

                empty_since = None

                valid_msgs = []

                for msg in msg_lst:
                    if msg.error():
                        self.handle_error(msg,msg.error())
                        continue
                    valid_msgs.append(msg)

                if not valid_msgs:
                    continue

                self.logger.info(f'message count:{len(msg_lst)}')
                for msg in msg_lst:
                    error = msg.error()
                    if error:
                        self.handle_error(msg,error)

                #kafka 메시징 큐로부터, 파라미터 추출
                self.logger.info(f'파라미터 추출 시작')

                msg_param_lst = [
                    json.loads(msg.value().decode('utf-8'))
                    for msg in msg_lst
                ]

                self.logger.info(msg_param_lst)

                start_time = time.time()

                group_results = defaultdict(list)

                with ThreadPoolExecutor(max_workers=5) as executor:
                    futures = [
                        executor.submit(MapleApiRequest,param)
                        for param in msg_param_lst
                    ]

                    for future in as_completed(futures):
                        self.logger.debug(future.result())

                        con,table_nm = future.result()
                        
                        if isinstance(con,list):
                            group_results[table_nm].extend(con)
                        else:
                            group_results[table_nm].append(con)


                self.logger.info(group_results)

                self.consumer.commit(asynchronous=False)

                elapsed_time = time.time() - start_time

                if elapsed_time < 1:
                    time.sleep(1-elapsed_time)





        except KafkaException:
            self.logger.exception("Kafka exception occurred during message consumption")

        except KeyboardInterrupt: #키보드 입력으로 종료시
            self.logger.info("Shutting down consumer due to keyboard interrupt.")

        finally:
            self.consumer.close()
            self.logger.info("Consumer closed.")


if __name__ =='__main__':
    airflow_kafka_consumer =AirflowKafkaConsumer('maple_request_consumer')
    airflow_kafka_consumer.poll()
