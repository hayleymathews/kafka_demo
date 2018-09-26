import time
import cv2
from kafka import KafkaProducer, SimpleProducer, KafkaClient

kafka = KafkaClient('localhost:9092')
producer = SimpleProducer(kafka)
topic = 'wrong_lever'


def video_emitter(video):
    video = cv2.VideoCapture(video)
    print('emitting')

    while video.isOpened():
        success, image = video.read()
        if not success:
            break
        ret, jpeg = cv2.imencode('.png', image)
        producer.send_messages(topic, jpeg.tobytes())
        time.sleep(0.2) # sleep to reduce CPU usage
    
    video.release()
    print('done emitting')


if __name__ == '__main__':
    video_emitter('wrong_lever.mp4')