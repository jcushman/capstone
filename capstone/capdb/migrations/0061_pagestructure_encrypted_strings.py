# Generated by Django 2.0.10 on 2019-03-27 19:22

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('capdb', '0060_auto_20190327_1416'),
    ]

    operations = [
        migrations.AddField(
            model_name='pagestructure',
            name='encrypted_strings',
            field=models.TextField(blank=True, null=True),
        ),
    ]
