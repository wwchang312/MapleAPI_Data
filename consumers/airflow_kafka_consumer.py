from base_consumer.BaseConsumer import BaseConsumer
from confluent_kafka import Consumer,KafkaException
import json


#airflow "publish_maple_api_request_dag"에서 publishing한 파라미터를 consume

class AirflowKafkaConsumer(BaseConsumer):
    def __init__(self, group_id):
        super().__init__(group_id)
        self.topics=['collect_maple_character_list_dag']

        conf = {
            'bootstrap.servers': self.BOOTSTRAP_SERVERS,
            'group.id': self.group_id,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': 'true'
        }

        self.consumer = Consumer(conf)
        self.consumer.subscribe(self.topics, on_assign=self.callback_on_assign)

    #consumer poll message
    def poll(self):
        try:
            while True:
                msg_lst = self.consumer.consume(num_messages=5,timeout=1.0)
                if msg_lst is None or len(msg_lst) == 0: continue

                self.logger.info(f'message count:{len(msg_lst)}')
                for msg in msg_lst:
                    error = msg.error()
                    if error:
                        self.handle_error(msg,error)

                #kafka 메시징 큐로부터, 파라미터 추출
                self.logger.info(f'파라미터 추출 시작')
                msg_param_lst = [json.loads(msg.value().decode('utf-8')) for msg in msg_lst]
                self.extract_param(msg_param_lst) #Param을 각 key의 value를 List 타입으로 바꾸는 함수

        except KafkaException:
            self.logger.exception("Kafka exception occurred during message consumption")

        except KeyboardInterrupt: #키보드 입력으로 종료시
            self.logger.info("Shutting down consumer due to keyboard interrupt.")

        finally:
            self.consumer.close()
            self.logger.info("Consumer closed.")

    #파라미터로 입력받은 값이 들어오기 때문에 형태가 고정된다. 따라서 첫번째 딕셔너리의 k값으로 리스트 안에 있는 딕셔너리의 값들을 중복없이 가져온다.
    def extract_param(self,lst):
        rst = {
            k: list(dict.fromkeys((d[k] for d in lst)))
            for k in lst[0]
        }
        return rst


if __name__ =='__main__':
    airflow_kafka_consumer =AirflowKafkaConsumer('maple_request_consumer')
    airflow_kafka_consumer.poll()
