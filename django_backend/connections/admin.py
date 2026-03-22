from django.contrib import admin
from .models import ConnectionProfile


@admin.register(ConnectionProfile)
class ConnectionProfileAdmin(admin.ModelAdmin):
    list_display = ('name', 'host', 'database_name', 'user', 'created_at')
    list_filter = ('user',)
    search_fields = ('host', 'database_name')
    readonly_fields = ('db_password',)