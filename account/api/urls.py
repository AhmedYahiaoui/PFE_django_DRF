from django.conf.urls import url
from django.urls import path, include

from account.api.views import (
    registration_view,
    account_properties_view,
    update_account_view)

from rest_framework.authtoken.views import obtain_auth_token

app_name = 'account'

urlpatterns = [

    path('register', registration_view, name="register"),
    path('login', obtain_auth_token, name="login"),  # -> see accounts/api/views.py for response and url info
    path('user', account_properties_view, name="properties"),
    path('user/update', update_account_view, name="update"),

]