from builtins import print

from kafka.errors import KafkaError
from rest_framework import status, generics
from rest_framework.response import Response
from rest_framework.decorators import api_view, permission_classes
from rest_framework.permissions import IsAuthenticated
from devices.models import devices, Datas
from django.db.models import Max, Min, Count, Sum, Prefetch

from devices.api.serializers import DevicesSerializer, DevicesSerializer2, TestSerializer
from devices.api.serializers import DevicesSerializerUser
from devices.api.serializers import DevicetestSerializer
from devices.api.serializers import CountDevicesSerializer
from kafka import KafkaProducer, KafkaConsumer, TopicPartition
from logpipe import Producer, Consumer, register_consumer

from aiokafka import AIOKafkaProducer, AIOKafkaConsumer

import json
import asyncio

import json
from time import sleep
from multiprocessing import Process

# Test musicien et album

from devices.api.serializers import DatasSerializer

import pickle

SUCCESS = 'success'
ERROR = 'error'
DELETE_SUCCESS = 'deleted'
UPDATE_SUCCESS = 'updated'
CREATE_SUCCESS = 'created'


# GET ALL
@api_view(['GET', ])
# @permission_classes((IsAuthenticated,))
def api_All_devices_view(request):
    try:
        device = devices.objects.all()
    except devices.DoesNotExist:
        return Response(status=status.HTTP_404_NOT_FOUND)

    if request.method == 'GET':
        # serializer = DevicesSerializerUser(device, many=True)
        serializer = DevicesSerializer(device, many=True)
        return Response(serializer.data)
    print(device.author)


# GET ALL by ID
@api_view(['GET', ])
@permission_classes((IsAuthenticated,))
def api_All_devices_par_id_view(request):
    try:
        device = devices.objects.all()
    except devices.DoesNotExist:
        return Response(status=status.HTTP_404_NOT_FOUND)

    if request.method == 'GET':
        user = request.user
        print("All devices are : ", device)
        print("************")
        device3 = devices.objects.filter(author=user.id)
        print("All devices by id  are : ", device3)
        serializer = DevicesSerializerUser(device3, many=True)
        return Response(serializer.data)


# GET PAR ID / Slug
@api_view(['GET', ])
@permission_classes((IsAuthenticated,))
def api_detail_device_view(request, slug):
    try:
        device = devices.objects.get(slug=slug)
    except devices.DoesNotExist:
        return Response(status=status.HTTP_404_NOT_FOUND)

    user = request.user
    if device.author != user:
        return Response({'response': "You don't have permission to See that data."})
    if request.method == 'GET':
        # serializer = DevicesSerializerUser(device)
        serializer = DevicetestSerializer(device)
        return Response(serializer.data)


@api_view(['PUT', ])
@permission_classes((IsAuthenticated,))
def api_update_device_view(request, slug):
    try:
        device = devices.objects.get(slug=slug)
    except devices.DoesNotExist:
        return Response(status=status.HTTP_404_NOT_FOUND)

    user = request.user
    if device.author != user:
        return Response({'response': "You don't have permission to edit that."})

    if request.method == 'PUT':
        serializer = DevicesSerializer(device, data=request.data)
        data = {}
        if serializer.is_valid():
            serializer.save()
            data['response'] = UPDATE_SUCCESS
            return Response(data=data)
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)


@api_view(['DELETE', ])
@permission_classes((IsAuthenticated,))
def api_delete_device_view(request, slug):
    try:
        device = devices.objects.get(slug=slug)
    except devices.DoesNotExist:
        return Response(status=status.HTTP_404_NOT_FOUND)

    user = request.user
    if device.author != user:
        return Response({'response': "You don't have permission to delete that."})

    if request.method == 'DELETE':
        operation = device.delete()
        data = {}
        if operation:
            data['response'] = DELETE_SUCCESS
        return Response(data=data)


@api_view(['POST'])
@permission_classes((IsAuthenticated,))
def api_create_device_view(request):
    device = devices(author=request.user)

    if request.method == 'POST':
        serializer = DevicesSerializer(device, data=request.data)
        data = {}
        if serializer.is_valid():
            serializer.save()
            return Response(serializer.data, status=status.HTTP_201_CREATED)
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)


@api_view(['POST'])
@permission_classes((IsAuthenticated,))
def api_create_device_without_image_view(request):
    device = devices(author=request.user)

    if request.method == 'POST':
        serializer = DevicesSerializer(device, data=request.data)
        data = {}
        if serializer.is_valid():
            serializer.save()
            return Response(serializer.data, status=status.HTTP_201_CREATED)
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)


# --------------------------------------------                      Data                                       -------------------------------------------------------------

# consumer = KafkaConsumer(
#     'people',
#     bootstrap_servers=['192.168.1.11:9092'],
#     group_id=None,
#     consumer_timeout_ms=1000,
#     auto_offset_reset='earliest',
#     enable_auto_commit = True,
#     auto_commit_interval_ms = 1000
#
#     )
# # lastOffset = consumer.end_offsets([tp])[tp]
#
# for msg in consumer:
#     message = msg.value
#     print (message)
#     # if message.offset == lastOffset - 1:
#     #     break
#     # sleep(5)







@api_view(['POST'])
def api_create_data(request):
    if request.method == 'POST':
        serializer = DatasSerializer(data=request.data)
        data = {}

        # print(joe)
        # consumer = Consumer('people', consumer_timeout_ms=1000)

        if serializer.is_valid():
            serializer.save()

            producer = Producer('people', TestSerializer)
            joe = Datas.objects.create(
                device_id_id=serializer.data['device_id'],
                lat=serializer.data['lat'],
                lng=serializer.data['lng'],
                x_acc=serializer.data['x_acc'],
                y_acc=serializer.data['y_acc'],
                z_acc=serializer.data['z_acc']
            )
            producer.send(joe)
            print(joe)
            # consumer.register(TestSerializer)
            # consumer.run()
            # producer = KafkaProducer(bootstrap_servers='localhost:9092')
            # producer.send('people', 'n9oulo sayé')
            return Response(serializer.data, status=status.HTTP_201_CREATED)
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)


class DataListView(generics.ListCreateAPIView):
    queryset = Datas.objects.all()
    serializer_class = DatasSerializer



class DataView(generics.RetrieveUpdateDestroyAPIView):
    # Kafka Prodocuder

    # producer = KafkaProducer(bootstrap_servers='localhost:9092')
    # producer.send('people', Datas.objects.all())

    serializer_class = DatasSerializer
    queryset = Datas.objects.all()



# --------------------------------------------                      TEST                                       -------------------------------------------------------------


# class api_create_device_Producer_view(generics.ListCreateAPIView):
#     joe = Datas.objects.create(device_id_id=38,
#                                lat=32.875176,
#                                lng=8.799482,
#                                x_acc=10,
#                                y_acc=10,
#                                z_acc=10)
#     producer = Producer('people', TestSerializer)
#     #
#     # serializer_class = TestSerializer
#     # producer = Producer('people', TestSerializer)
#     producer.send(joe)


# --------------------------------------------                      TEST                                       -------------------------------------------------------------


# GET ALL data  by ID and 1 device
@api_view(['GET', ])
@permission_classes((IsAuthenticated,))
def api_All_data_by_device_view(request, slug):
    try:
        device = devices.objects.get(slug=slug)
    except devices.DoesNotExist:
        return Response(status=status.HTTP_404_NOT_FOUND)
    user = request.user
    if device.author != user:
        return Response({'response': "You don't have permission to see that data."})
    if request.method == 'GET':
        # device3 = devices.objects.filter(author=user.id).order_by('date_updated')

        data1 = Datas.objects.filter(device_id=device).order_by('date_updated').reverse()
        serializer = DatasSerializer(data1, many=True)
        return Response(serializer.data)


# GET ALL by ID
@api_view(['GET', ])
@permission_classes((IsAuthenticated,))
def api_All_data_All_devices_view(request):
    try:
        device = devices.objects.all()
    except devices.DoesNotExist:
        return Response(status=status.HTTP_404_NOT_FOUND)
    if request.method == 'GET':
        user = request.user
        data1 = Datas.objects.filter(device_id__author=user.id)
        serializer2 = DatasSerializer(data1, many=True)
        dataas = Datas.objects.filter(device_id__author=user.id).distinct().values('device_id')
        device3 = devices.objects.filter(author=user.id).prefetch_related(
            Prefetch('device_data', queryset=Datas.objects.order_by('-date_updated')))

        # print("device_id__title : ", data1)
        print("All devices by id  are : ", device3)
        serializer = DevicesSerializer2(device3, many=True)

        # tcu_pos = Datas.objects.filter(device_id__author=user.id).values('lat', 'lng').order_by('-id')[0]
        # print("tcu_pos : ", tcu_pos)
        #
        # resulat = []
        # for x in device:
        #     resulat.append(x.device_data)
        # print("returne : ", resulat)
        #
        # positions = [
        #     # devices.datas_set.order_by('-id').values('lat', 'lng')[0]
        #     # for devices in request.user.devices_set.prefetch_related('datas_set')
        # ]
        #
        # returne = data1.select_related('device_id')[0]
        # print("returne : ", returne)

        return Response(serializer.data)


# ----------------------------------------------- FILTER ---------------------------------------


# GET ALL by USER -> Category  ******* humains ************
@api_view(['GET', ])
@permission_classes((IsAuthenticated,))
def api_All_devices_par_id_humains(request):
    try:
        device = devices.objects.all()
    except devices.DoesNotExist:
        return Response(status=status.HTTP_404_NOT_FOUND)

    if request.method == 'GET':
        user = request.user
        device3 = devices.objects.filter(author=user.id).filter(category="humains")
        print("All devices by id  are : ", device3)
        serializer = DevicesSerializerUser(device3, many=True)
        return Response(serializer.data)


# GET ALL by USER -> Category ******* animals ************
@api_view(['GET', ])
@permission_classes((IsAuthenticated,))
def api_All_devices_par_id_animals(request):
    try:
        device = devices.objects.all()
    except devices.DoesNotExist:
        return Response(status=status.HTTP_404_NOT_FOUND)

    if request.method == 'GET':
        user = request.user
        device3 = devices.objects.filter(author=user.id).filter(category="animals")
        print("All devices by id  are : ", device3)
        serializer = DevicesSerializerUser(device3, many=True)
        return Response(serializer.data)


# GET ALL by USER -> Category ******* object ************
@api_view(['GET', ])
@permission_classes((IsAuthenticated,))
def api_All_devices_par_id_object(request):
    try:
        device = devices.objects.all()
    except devices.DoesNotExist:
        return Response(status=status.HTTP_404_NOT_FOUND)

    if request.method == 'GET':
        user = request.user
        device3 = devices.objects.filter(author=user.id).filter(category="object")
        print("All devices by id  are : ", device3)
        serializer = DevicesSerializerUser(device3, many=True)
        return Response(serializer.data)


# GET ALL by USER -> Category ******* car ************
@api_view(['GET', ])
@permission_classes((IsAuthenticated,))
def api_All_devices_par_id_car(request):
    try:
        device = devices.objects.all()
    except devices.DoesNotExist:
        return Response(status=status.HTTP_404_NOT_FOUND)

    if request.method == 'GET':
        user = request.user
        device3 = devices.objects.filter(author=user.id).filter(category="car")
        print("All devices by id  are : ", device3)
        serializer = DevicesSerializerUser(device3, many=True)
        return Response(serializer.data)


# GET ALL by USER ->  ******* Favorit ************
@api_view(['GET', ])
@permission_classes((IsAuthenticated,))
def api_All_devices_by_favorit(request):
    try:
        device = devices.objects.all()
    except devices.DoesNotExist:
        return Response(status=status.HTTP_404_NOT_FOUND)

    if request.method == 'GET':
        user = request.user
        device3 = devices.objects.filter(author=user.id).filter(favorit=1)
        print("All devices by favorit  are : ", device3)
        serializer = DevicesSerializerUser(device3, many=True)
        return Response(serializer.data)


# COUNT ALL by USER -> Category ******************
@api_view(['GET', ])
@permission_classes((IsAuthenticated,))
def api_count_devices_par_id(request):
    try:
        device = devices.objects.all()
    except devices.DoesNotExist:
        return Response(status=status.HTTP_404_NOT_FOUND)

    if request.method == 'GET':
        user = request.user

        All_Devices = devices.objects.filter(author=user.id).count()
        device_humains = devices.objects.filter(author=user.id).filter(category="humains").count()
        device_animals = devices.objects.filter(author=user.id).filter(category="animals").count()
        device_cars = devices.objects.filter(author=user.id).filter(category="car").count()
        device_objects = devices.objects.filter(author=user.id).filter(category="object").count()
        listDevices = ["All devices   are : ", All_Devices,
                       "All devices device_humains  are : ", device_humains,
                       "All devices device_animals  are : ", device_animals,
                       "All devices device_cars  are : ", device_cars,
                       "All devices device_objects  are : ", device_objects
                       ]
        return Response(listDevices)


# ----------------------------------------------- ODER BY  ---------------------------------------


# GET ALL
@api_view(['GET', ])
@permission_classes((IsAuthenticated,))
def api_All_devices_date(request):
    try:
        device = devices.objects.all()
    except devices.DoesNotExist:
        return Response(status=status.HTTP_404_NOT_FOUND)

    if request.method == 'GET':
        device3 = devices.objects.order_by('date_published')
        # Reserved.objects.filter(client=client_id).order_by('date_published').reverse()
        print(device3)

        serializer = DevicesSerializer(device3, many=True)
        return Response(serializer.data)


@api_view(['GET', ])
@permission_classes((IsAuthenticated,))
def api_All_devices_date_asc(request):
    try:
        device = devices.objects.all()
    except devices.DoesNotExist:
        return Response(status=status.HTTP_404_NOT_FOUND)

    if request.method == 'GET':
        user = request.user
        print("All devices are : ", device)
        print("************")
        device3 = devices.objects.filter(author=user.id).order_by('date_published')
        print("All devices by id  are : ", device3)
        serializer = DevicesSerializerUser(device3, many=True)
        return Response(serializer.data)


@api_view(['GET', ])
@permission_classes((IsAuthenticated,))
def api_All_devices_date_desc(request):
    try:
        device = devices.objects.all()
    except devices.DoesNotExist:
        return Response(status=status.HTTP_404_NOT_FOUND)

    if request.method == 'GET':
        user = request.user
        print("All devices are : ", device)
        print("************")
        device3 = devices.objects.filter(author=user.id).order_by('date_published').reverse()
        print("All devices by id  are : ", device3)
        serializer = DevicesSerializerUser(device3, many=True)
        return Response(serializer.data)


# ********************************************************************


# !/usr/bin/env python
import threading, logging, time
import multiprocessing


class Consumer(multiprocessing.Process):
    def __init__(self):
        multiprocessing.Process.__init__(self)
        self.stop_event = multiprocessing.Event()

    def stop(self):
        self.stop_event.set()

    def run(self):
        consumer = KafkaConsumer(bootstrap_servers='localhost:9092',
                                 auto_offset_reset='earliest',
                                 consumer_timeout_ms=1000)
        consumer.subscribe(['people'])

        while not self.stop_event.is_set():
            for message in consumer:
                print(message)
                if self.stop_event.is_set():
                    break

        consumer.close()
