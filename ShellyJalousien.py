from __future__ import annotations

import logging
from pathlib import Path
import time
import os
import paho.mqtt.client as pahoMqtt
from PythonLib.JsonUtil import JsonUtil
from PythonLib.Mqtt import Mqtt
from PythonLib.DateUtil import DateTimeUtilities
from PythonLib.MqttConfigContainer import MqttConfigContainer
from PythonLib.Scheduler import Scheduler
from PythonLib.TaskQueue import TaskQueueExecuteOperation, TaskQueue, TaskQueueWait


logger = logging.getLogger('ShellyJalousien')

DATA_PATH = Path(os.getenv('DATA_PATH', "."))

CONFIG = {
    "Wohnzimmer":
    {
        "topic": "/house/garden/markiese/wohnzimmer/command/cover:0"
    },
    "Arbeitszimmer":
    {
        "topic": "/house/garden/markiese/arbeitszimmer/command/cover:0"
    }
}


class Module:
    def __init__(self) -> None:
        self.scheduler = Scheduler()
        self.mqttClient = Mqtt("koserver.iot", "/house/agents/ShellyJalousien", pahoMqtt.Client("ShellyJalousien"))
        self.config = MqttConfigContainer(self.mqttClient, "/house/agents/ShellyJalousien/config", DATA_PATH.joinpath("ShellyJalousien.json"), CONFIG)

    def getConfig(self) -> MqttConfigContainer:
        return self.config

    def getScheduler(self) -> Scheduler:
        return self.scheduler

    def getMqttClient(self) -> Mqtt:
        return self.mqttClient

    def setup(self) -> None:
        self.scheduler.scheduleEach(self.mqttClient.loop, 500)
        self.scheduler.scheduleEach(self.config.loop, 60000)

    def loop(self) -> None:
        self.scheduler.loop()


class ShellyJalousien:

    def __init__(self, module: Module) -> None:
        self.configContainer = module.getConfig()
        self.mqttClient = module.getMqttClient()
        self.scheduler = module.getScheduler()
        self.config = {}

    def setup(self) -> None:

        self.configContainer.setup()
        self.configContainer.subscribeToConfigChange(self.__updateConfig)

        self.mqttClient.subscribeStartWithTopic('/house/agents/ShellyJalousien/set/', self.__receiveData)
        self.scheduler.scheduleEach(self.__keepAlive, 10000)

    def __receiveData(self, topic: str, payload: str) -> None:

        try:

            jalousienName = Path(topic).name
            command = payload
            config = self.config.get(jalousienName)
            if config:
                taskQueue = config.get('taskQueue')

                if not taskQueue:
                    taskQueue = TaskQueue()
                    config['taskQueue'] = taskQueue

                topicToSwitch = config['topic']

                match(command):
                    case 'open':
                        taskQueue.add(TaskQueueExecuteOperation(lambda: self.mqttClient.publishIndependentTopic(topicToSwitch, 'stop')))
                        taskQueue.add(TaskQueueWait(1000))
                        taskQueue.add(TaskQueueExecuteOperation(lambda: self.mqttClient.publishIndependentTopic(topicToSwitch, 'open')))
                    case 'stop':
                        taskQueue.add(TaskQueueExecuteOperation(lambda: self.mqttClient.publishIndependentTopic(topicToSwitch, 'stop')))
                    case 'close':
                        taskQueue.add(TaskQueueExecuteOperation(lambda: self.mqttClient.publishIndependentTopic(topicToSwitch, 'stop')))
                        taskQueue.add(TaskQueueWait(1000))
                        taskQueue.add(TaskQueueExecuteOperation(lambda: self.mqttClient.publishIndependentTopic(topicToSwitch, 'close')))

        except BaseException:
            logging.exception('')

    def __updateConfig(self, config: dict) -> None:
        self.config = config

    def __keepAlive(self) -> None:
        self.mqttClient.publishIndependentTopic('/house/agents/ShellyJalousien/heartbeat', DateTimeUtilities.getCurrentDateString())
        self.mqttClient.publishIndependentTopic('/house/agents/ShellyJalousien/subscriptions', JsonUtil.obj2Json(self.mqttClient.getSubscriptionCatalog()))


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    logging.getLogger('ShellyJalousien').setLevel(logging.DEBUG)

    module = Module()
    module.setup()

    ShellyJalousien(module).setup()

    print("ShellyJalousien is running!")

    while (True):
        module.loop()
        time.sleep(0.25)


if __name__ == '__main__':
    main()
