# Generated by Django 2.2.11 on 2020-03-23 17:32

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('capdb', '0096_auto_20200317_1957'),
    ]

    operations = [
        migrations.RenameField(
            model_name='extractedcitation',
            old_name='cite_original',
            new_name='cite',
        ),
    ]