import boto3
from PIL import Image
from io import BytesIO
import csv
import os
from concurrent.futures import ThreadPoolExecutor, as_completed


def check_image_validity_and_color(s3_client, bucket_name, s3_key):
    try:
        # Fetch the image from S3
        response = s3_client.get_object(Bucket=bucket_name, Key=s3_key)
        
        # Check if the Body is empty
        image_data = response.get('Body')
        if image_data is None:
            print(f"Error: Image data for {s3_key} is None")
            return (False, s3_key, False)  # Invalid image
        
        image_data = image_data.read()
        if not image_data:
            print(f"Error: Empty image data for {s3_key}")
            return (False, s3_key, False)  # Invalid image
        
        # Attempt to open the image using Pillow
        img = Image.open(BytesIO(image_data))
        img.verify()  # Verify if the image is valid
        img = img.convert('RGB')  # Ensure the image is in RGB mode
        pixels = list(img.getdata())

        # Check if all pixels match the first pixel (black or white)
        first_pixel = pixels[0]
        for pixel in pixels:
            if pixel != first_pixel:
                is_one_color = False
                break
        else:
            is_one_color = True

        return (True, s3_key, is_one_color)

    except Exception as e:
        print(f"Error with image {s3_key}: {e}")
        return (False, s3_key, False)  # Invalid image


def process_images_in_parallel(bucket_name, folder_path, aws_access_key, aws_secret_key, aws_region):
    s3_client = boto3.client(
        's3',
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key,
        region_name=aws_region
    )

    imageCount = 0
    one_color_images = []  # List to store black/white images
    invalid_images = []  # List to store invalid images
    continuation_token = None

    futures = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        try:
            while True:
                # List objects with pagination support
                list_params = {
                    'Bucket': bucket_name,
                    'Prefix': folder_path
                }

                if continuation_token:
                    list_params['ContinuationToken'] = continuation_token

                response = s3_client.list_objects_v2(**list_params)
                if 'Contents' not in response:
                    print(f"No images found in folder {folder_path}.")
                    break

                for obj in response['Contents']:
                    s3_key = obj['Key']
                    # Filter out directories (if any)
                    if not s3_key.endswith('/'):
                        imageCount += 1
                        print(f"{imageCount}) Queuing image: {s3_key}")
                        futures.append(executor.submit(check_image_validity_and_color, s3_client, bucket_name, s3_key))

                # Check for continuation token for paginated results
                continuation_token = response.get('NextContinuationToken', None)
                if not continuation_token:
                    break

        except Exception as e:
            print(f"Error while listing objects: {e}")

        # Process results from futures
        for future in as_completed(futures):
            is_valid, s3_key, is_one_color = future.result()

            if is_valid:
                image_url = f"s3://{bucket_name}/{s3_key}"
                if is_one_color:
                    one_color_images.append(image_url)
                else:
                    print(f"Image {s3_key} is not black/white.")
            else:
                invalid_images.append(f"s3://{bucket_name}/{s3_key}")

    return one_color_images, invalid_images


def save_images_to_csv(black_white_images, invalid_images, black_white_csv, invalid_images_csv):
    # Ensure the CSV file paths are valid
    if not os.path.exists(os.path.dirname(black_white_csv)):
        print(f"The directory {os.path.dirname(black_white_csv)} does not exist.")
        return

    # Write black/white image URLs to CSV
    if black_white_images:
        with open(black_white_csv, mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(['Black/White Image URL'])
            for image_url in black_white_images:
                writer.writerow([image_url])  # Write each black/white image URL
        print(f"Black or White images have been saved to {black_white_csv}")
    else:
        print("No Black or White images found.")

    if invalid_images:
        with open(invalid_images_csv, mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(['Invalid Image URL'])  # Header row
            for image_url in invalid_images:
                writer.writerow([image_url])  # Write each invalid image URL
        print(f"Invalid images have been saved to {invalid_images_csv}")
    else:
        print("No invalid images found.")


# Input values
bucket_name = ''
folder_path = ''
aws_access_key = ''
aws_secret_key = ''
aws_region = ''
black_white_csv = 'path'  # Output CSV for black or white images
invalid_images_csv = 'path'  # Output CSV for invalid images

# Call the function to process images
one_color_images, invalid_images = process_images_in_parallel(bucket_name, folder_path, aws_access_key, aws_secret_key, aws_region)

# Save results to CSV files
save_images_to_csv(one_color_images, invalid_images, black_white_csv, invalid_images_csv)
