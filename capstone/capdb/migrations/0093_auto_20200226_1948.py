# Generated by Django 2.2.10 on 2020-02-26 19:48

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('capdb', '0092_auto_20200225_1511'),
    ]

    operations = [
        migrations.AddField(
            model_name='casemetadata',
            name='first_page_order',
            field=models.SmallIntegerField(blank=True, help_text='1-based page order of first page', null=True),
        ),
        migrations.AddField(
            model_name='casemetadata',
            name='last_page_order',
            field=models.SmallIntegerField(blank=True, help_text='1-based page order of last page', null=True),
        ),
        migrations.AddField(
            model_name='historicalcasemetadata',
            name='first_page_order',
            field=models.SmallIntegerField(blank=True, help_text='1-based page order of first page', null=True),
        ),
        migrations.AddField(
            model_name='historicalcasemetadata',
            name='last_page_order',
            field=models.SmallIntegerField(blank=True, help_text='1-based page order of last page', null=True),
        ),
        migrations.AlterField(
            model_name='casemetadata',
            name='first_page',
            field=models.CharField(blank=True, help_text='Label of first page', max_length=255, null=True),
        ),
        migrations.AlterField(
            model_name='casemetadata',
            name='last_page',
            field=models.CharField(blank=True, help_text='Label of first page', max_length=255, null=True),
        ),
        migrations.AlterField(
            model_name='historicalcasemetadata',
            name='first_page',
            field=models.CharField(blank=True, help_text='Label of first page', max_length=255, null=True),
        ),
        migrations.AlterField(
            model_name='historicalcasemetadata',
            name='last_page',
            field=models.CharField(blank=True, help_text='Label of first page', max_length=255, null=True),
        ),
    ]
