from django.db import models
from django.utils.text import slugify
from django.conf import settings
from django.db.models.signals import post_delete, pre_save
from django.dispatch import receiver


def upload_location(instance, filename, **kwargs):
    file_path = 'devices/{author_id}/{title}-{filename}'.format(
        author_id=str(instance.author.id), title=str(instance.title), filename=filename
    )
    return file_path


class devices(models.Model):
    title = models.CharField(max_length=50, null=False, blank=False)
    image = models.ImageField(upload_to=upload_location, null=True, blank=True)
    #image = models.ImageField(upload_to=upload_location, null=False, blank=False)
    favorit = models.BooleanField(null=False, blank=False ,default = 0)
    # ref = models.IntegerField(null=False, blank=False)
    ref = models.CharField(max_length=20, null=False, blank=False)
    date_published = models.DateTimeField(auto_now_add=True, verbose_name="date published")
    category = models.CharField(max_length=10, null=False, blank=False)
    author = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.CASCADE)
    slug = models.SlugField(blank=True, unique=True)

    def __str__(self):
        return self.title


@receiver(post_delete, sender=devices)
def submission_delete(sender, instance, **kwargs):
    instance.image.delete(False)


def pre_save_device_post_receiever(sender, instance, *args, **kwargs):
    if not instance.slug:
        instance.slug = slugify(instance.author.username + "-" + instance.title)


pre_save.connect(pre_save_device_post_receiever, sender=devices)






class Datas(models.Model):

    device_id = models.ForeignKey(devices, on_delete=models.CASCADE, related_name='device_data')

    lat = models.FloatField(max_length=10, null=False, blank=False)
    lng = models.FloatField(max_length=10, null=False, blank=False)
    x_acc = models.FloatField(max_length=10, null=False, blank=False)
    y_acc = models.FloatField(max_length=10, null=False, blank=False)
    z_acc = models.FloatField(max_length=10, null=False, blank=False)
    battery = models.FloatField(max_length=10, null=False, blank=False)
    date_updated = models.DateTimeField(auto_now=True, verbose_name="date updated")


    def __str__(self):
        return '%d: %d' % (self.lat, self.lng)