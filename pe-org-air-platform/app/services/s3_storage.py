from app.config import settings
 
import boto3
from botocore.exceptions import ClientError
 
 
def ping_s3() -> tuple[bool, str]:
    # If bucket not configured, treat as "not configured" (not a hard failure for local dev)
    if not settings.s3_bucket_name:
        return True, "not_configured"
 
    try:
        client = boto3.client(
            "s3",
            region_name=settings.aws_region,
            aws_access_key_id=settings.aws_access_key_id,
            aws_secret_access_key=settings.aws_secret_access_key,
        )
        client.head_bucket(Bucket=settings.s3_bucket_name)
        return True, "ok"
    except ClientError as e:
        return False, f"ClientError: {e}"
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"