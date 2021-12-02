from django.urls import path
from devices.api.views import (
    api_detail_device_view,
    api_update_device_view,
    api_delete_device_view,
    api_create_device_view,

    api_All_devices_view,
    api_All_devices_par_id_view,
    api_All_devices_par_id_animals,
    api_All_devices_par_id_car,
    api_All_devices_par_id_humains,
    api_All_devices_par_id_object,

    DataListView,
    DataView,

    api_All_devices_date_desc,
    api_All_devices_date_asc,
    api_count_devices_par_id,

    api_All_data_by_device_view,
    api_All_devices_by_favorit,
    api_All_data_All_devices_view,
    api_create_data
    # api_create_device_Producer_view
)

app_name = 'blog'

urlpatterns = [
    path('<slug>/', api_detail_device_view, name="detail"),
    path('<slug>/update', api_update_device_view, name="update"),
    path('<slug>/delete', api_delete_device_view, name="delete"),
    path('create', api_create_device_view, name="create"),


    path('All_Device', api_All_devices_view, name="all"),
    path('Get_All_Device_By_User', api_All_devices_par_id_view, name="all_devices_by_id"),


    path('data', DataListView.as_view(), name="devices data"),
    path('data/<pk>', DataView.as_view(), name="detail data"),

    # path('data2/<pk>', api_create_device_Producer_view.as_view(), name="detaildata2"),
    path('Add_data', api_create_data, name="devicess data"),


# Filter by category

    path('Get_All_devices_By_User_animals', api_All_devices_par_id_animals, name="api_All_devices_par_id_animals"),
    path('Get_All_devices_By_User_car', api_All_devices_par_id_car, name="api_All_devices_par_id_car"),
    path('Get_All_devices_By_User_humains', api_All_devices_par_id_humains, name="api_All_devices_par_id_humains"),
    path('Get_All_devices_By_User_object', api_All_devices_par_id_object, name="api_All_devices_par_id_object"),


# order by date

    path('Get_All_devices_By_User_date_asc', api_All_devices_date_asc, name="api_All_devices_par_date_asc"),
    path('Get_All_devices_By_User_date_desc', api_All_devices_date_desc, name="api_All_devices_par_date_desc"),

# Count by category
    path('Get_count_devices_By_User', api_count_devices_par_id, name="api_count_devices_par_id"),


# SHOW DATA BY SLUG wala BY id ( user )
    path('All_data_by_device/<slug>', api_All_data_by_device_view, name="api_count_devices_par_id"),

# SHOW DATA ALL DEVICES ( user )
    path('All_data_All_devices', api_All_data_All_devices_view, name="api_count_devices_par_id"),


# SHOW Device with Favorit == true ( user )
    path('Get_All_Favorit_devices', api_All_devices_by_favorit, name="api_Get_All_Favorit_devices_"),



]
