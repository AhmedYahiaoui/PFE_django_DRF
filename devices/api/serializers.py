from logpipe import Consumer
from rest_framework import serializers
from django.db import models

from devices.models import devices, Datas


# SHOW DEVICE

class DevicesSerializer(serializers.ModelSerializer):
    class Meta:
        model = devices
        fields = ['title','date_published','image','category', 'id','slug','ref','favorit']



# SHOW DATA

class DatasSerializer(serializers.ModelSerializer):

    class Meta:
        model = Datas
        fields = ['id','device_id','lat', 'lng','x_acc','y_acc','z_acc','battery','date_updated']





# Afficher USER
class DevicesSerializerUser(serializers.ModelSerializer):

    username = serializers.SerializerMethodField('get_username_from_author')

    class Meta:
        model = devices
        fields = ['title','date_published','image',
                  'category','username','author','slug','favorit','ref']

    def get_username_from_author(self,device):
        username = device.author.username
        return username



# ------ for test 1



class DevicetestSerializer(serializers.ModelSerializer):
    device_data = DatasSerializer(many=True)
    class Meta:
        model = devices
        fields = ['title', 'date_published','image','device_data','id']



#for count
class CountDevicesSerializer(serializers.ModelSerializer):
    class Meta:
        model = devices
        fields = ['id']



# SHOW DEVICE2
class DevicesSerializer2(serializers.ModelSerializer):
    device_data = DatasSerializer(many=True)
    class Meta:
        model = devices
        fields = ['title','date_published','image','category', 'id','slug','ref','favorit','device_data']



# SHOW DATA

class TestSerializer(serializers.ModelSerializer):
    MESSAGE_TYPE = 'datas'
    VERSION = 1
    KEY_FIELD = 'id'

    class Meta:
        model = Datas
        fields = ['id','device_id','lat', 'lng','x_acc','y_acc','z_acc','battery','date_updated']

#     @classmethod
#     def lookup_instance(cls, uuid, **kwargs):
#         try:
#             return Datas.objects.get(uuid=uuid)
#         except models.Datas.DoesNotExist:
#             pass
# consumer = Consumer('people', consumer_timeout_ms=1000)
# consumer.register(TestSerializer)