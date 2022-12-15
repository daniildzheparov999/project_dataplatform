from rest_framework import serializers
from EmployeeApp.models import Departments, Employees, Worklogs

class DepartmentSerializer(serializers.ModelSerializer):
          class Meta:
                    model=Departments
                    fields=('DepartmentId', 'DepartmentName')

class EmployeeSerializer(serializers.ModelSerializer):
          class Meta:
                    model=Employees
                    fields=('EmployeeId', 'EmployeeName', 'Department', 'DateOfJoining')

class WorklogSerializer(serializers.ModelSerializer):
          class Meta:
                    model=Worklogs
                    fields=('WorklogId', 'EmployeeId', 'WorklogDate', 'WorklogHours')
