from django.contrib import admin

# Register your models here.
from devices.models import devices

admin.site.register(devices)