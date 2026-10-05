import boto3
import os
from dotenv import load_dotenv

# Load variables from the .env file located in the project root
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '..', '.env'))

def verify_s3_buckets():
    """
    Authenticates with AWS and verifies the existence of Medallion architecture buckets.
    """
    # Initialize the low-level S3 client.
    # Boto3 automatically intercepts AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY 
    # from the environment variables loaded by dotenv.
    s3_client = boto3.client('s3', region_name=os.getenv('AWS_DEFAULT_REGION'))
    
    # Fetch all buckets associated with the IAM user
    response = s3_client.list_buckets()
    available_buckets = [bucket['Name'] for bucket in response.get('Buckets', [])]
    
    # Define the exact buckets required for the pipeline to function
    required_buckets = [
        os.getenv('BRONZE_BUCKET'),
        os.getenv('SILVER_BUCKET'),
        os.getenv('GOLD_BUCKET')
    ]
    
    print("\n--- AWS S3 Medallion Storage Check ---")
    all_passed = True
    for bucket in required_buckets:
        if bucket in available_buckets:
            print(f"✅ Verified: {bucket}")
        else:
            print(f"❌ Missing: {bucket}")
            all_passed = False
            
    if all_passed:
        print("\n🚀 SUCCESS: Phase 1 Infrastructure and AWS connectivity verified.\n")
    else:
        print("\n⚠ FAILURE: One or more required buckets are missing. Check AWS console or .env.\n")

if __name__ == "__main__":
    verify_s3_buckets()