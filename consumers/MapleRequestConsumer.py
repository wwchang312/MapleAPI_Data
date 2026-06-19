from base_consumer.BaseConsumer import BaseConsumer
from confluent_kafka import Consumer,KafkaException
import os
import json


#메이플스토리 OpenAPI 호출을 위한 공통된 url 정보를 활용하기 위한 basic consumer 생성

api_key = os.environ['NEXON_API_KEY']

class MapleRequestConsumer(BaseConsumer):
    def __init__(self, group_id):
        super().__init__(group_id)
        self.topics=['collect_maple_character_list_dag']

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
            while True:
                msg_lst = self.consumer.consume()
                if msg_lst is None or len(msg_lst) == 0: continue

                self.logger.info(f'message count:{len(msg_lst)}')
                for msg in msg_lst:
                    error = msg.error()
                    if error:
                        self.handle_error(msg,error)

                #kafka 메시징 큐로부터, 파라미터 추출
                self.logger.info(f'파라미터 추출 시작')
                msg_param_lst = [json.loads(msg.value().decode('utf-8')) for msg in msg_lst]

                # print(msg_param_lst)
                print(msg_param_lst[0])
                print(msg_param_lst[0].get('character_name'))

        except KafkaException:
            self.logger.exception("Kafka exception occurred during message consumption")

        except KeyboardInterrupt: #키보드 입력으로 종료시
            self.logger.info("Shutting down consumer due to keyboard interrupt.")

        finally:
            self.consumer.close()
            self.logger.info("Consumer closed.")

if __name__ =='__main__':
    maple_request_consumer =MapleRequestConsumer('maple_request_consumer')
    maple_request_consumer.poll()
