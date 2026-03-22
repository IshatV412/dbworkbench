from django.db import transaction
from .models import CommitEvent, InverseOperation, Snapshot, SnapshotPolicy


def record_commit(version_id, sql_command, inverse_sql, user, connection_profile, status):
    with transaction.atomic():
        commit = CommitEvent.objects.create(
            version_id=version_id,
            sql_command=sql_command,
            status=status,
            user=user,
            connection_profile=connection_profile,
        )

        InverseOperation.objects.create(
            version_id=version_id,
            inverse_sql=inverse_sql,
            commit=commit,
        )

        try:
            policy = SnapshotPolicy.objects.get(connection_profile=connection_profile)
        except SnapshotPolicy.DoesNotExist:
            return commit

        last_snapshot = Snapshot.objects.filter(
            connection_profile=connection_profile,
        ).order_by('-created_at').first()

        if last_snapshot:
            commits_since = CommitEvent.objects.filter(
                connection_profile=connection_profile,
                timestamp__gt=last_snapshot.created_at,
            ).count()
        else:
            commits_since = CommitEvent.objects.filter(
                connection_profile=connection_profile,
            ).count()

        if commits_since >= policy.frequency:
            Snapshot.objects.create(
                version_id=version_id,
                s3_key=f"snapshots/{connection_profile.id}/{version_id}",
                connection_profile=connection_profile,
            )

        return commit