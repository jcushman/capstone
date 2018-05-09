# Generated by Django 2.0.2 on 2018-04-20 01:17

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('capapi', '0001_initial'),
    ]

    operations = [
        migrations.RenameField(
            model_name='capuser',
            old_name='key_expires',
            new_name='nonce_expires',
        ),
        migrations.AddField(
            model_name='capuser',
            name='email_verified',
            field=models.BooleanField(default=False, help_text='Whether user has verified their email address'),
        ),
        migrations.AlterField(
            model_name='capuser',
            name='case_allowance_remaining',
            field=models.IntegerField(default=0),
        ),
        migrations.AlterField(
            model_name='capuser',
            name='is_active',
            field=models.BooleanField(default=True),
        ),
        migrations.AlterField(
            model_name='capuser',
            name='total_case_allowance',
            field=models.IntegerField(blank=True, default=0, null=True),
        ),
    ]
