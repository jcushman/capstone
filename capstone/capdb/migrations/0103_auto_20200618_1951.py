# Generated by Django 2.2.13 on 2020-06-18 19:51

import capdb.storages
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('capdb', '0102_auto_20200618_1942'),
    ]

    operations = [
        migrations.AlterField(
            model_name='casemetadata',
            name='name_abbreviation',
            field=models.CharField(blank=True, max_length=1024),
        ),
        migrations.AlterField(
            model_name='historicalcasemetadata',
            name='name_abbreviation',
            field=models.CharField(blank=True, max_length=1024),
        ),
        migrations.AlterField(
            model_name='volumemetadata',
            name='pdf_file',
            field=models.FileField(blank=True, help_text='Exported volume PDF', max_length=1000, storage=capdb.storages.DownloadOverlayStorage(base_url='http://case.test:8000/download/', location='/app/test_data/downloads'), upload_to=''),
        ),
    ]
