# Generated by Django 4.1.3 on 2022-12-13 22:25

from django.db import migrations, models


class Migration(migrations.Migration):

    initial = True

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='Departments',
            fields=[
                ('DepartmentId', models.AutoField(primary_key=True, serialize=False)),
                ('DepartmentName', models.CharField(max_length=500)),
            ],
        ),
        migrations.CreateModel(
            name='Employees',
            fields=[
                ('EmployeeId', models.AutoField(primary_key=True, serialize=False)),
                ('EmployeeName', models.CharField(max_length=500)),
                ('Department', models.CharField(max_length=500)),
                ('DateOfJoining', models.DateField()),
                ('PhotoFileName', models.CharField(max_length=500)),
            ],
        ),
        migrations.CreateModel(
            name='Worklogs',
            fields=[
                ('WorklogId', models.AutoField(primary_key=True, serialize=False)),
                ('EmployeeId', models.CharField(max_length=500)),
                ('WorklogDate', models.DateField()),
                ('WorklogHours', models.CharField(max_length=500)),
            ],
        ),
    ]
