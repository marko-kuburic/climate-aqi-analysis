#!/bin/bash

# ==============================================================================
# CDS API Setup Script
# ==============================================================================
# This script helps you configure the Copernicus Climate Data Store (CDS) API
# credentials for ERA5-Land climate data downloads.
#
# Prerequisites:
# 1. Register at: https://cds.climate.copernicus.eu/
# 2. Get your API key from: https://cds.climate.copernicus.eu/api-how-to
# 3. Accept the Terms and Conditions for ERA5-Land dataset
# ==============================================================================

set -e

echo "=============================================="
echo "CDS API Configuration Setup"
echo "=============================================="
echo ""

# Check if .cdsapirc already exists
CDSAPIRC="$HOME/.cdsapirc"

if [ -f "$CDSAPIRC" ]; then
    echo "⚠️  Warning: $CDSAPIRC already exists!"
    echo ""
    cat "$CDSAPIRC"
    echo ""
    read -p "Do you want to overwrite it? (y/N): " -n 1 -r
    echo ""
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo "Keeping existing configuration. Exiting."
        exit 0
    fi
fi

echo ""
echo "Please enter your CDS API credentials."
echo "You can find them at: https://cds.climate.copernicus.eu/api-how-to"
echo ""

# Prompt for UID
read -p "Enter your CDS UID (numeric, e.g., 123456): " CDS_UID

# Prompt for API Key
read -p "Enter your CDS API Key (format: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx): " CDS_KEY

echo ""
echo "Creating $CDSAPIRC..."

# Create the .cdsapirc file
cat > "$CDSAPIRC" << EOF
url: https://cds.climate.copernicus.eu/api/v2
key: ${CDS_UID}:${CDS_KEY}
EOF

# Set proper permissions
chmod 600 "$CDSAPIRC"

echo "✅ Configuration file created successfully!"
echo ""
echo "File location: $CDSAPIRC"
echo "File contents:"
cat "$CDSAPIRC"
echo ""

# Test the configuration
echo "=============================================="
echo "Testing CDS API connection..."
echo "=============================================="
echo ""

python3 - << 'PYEOF'
import sys
try:
    import cdsapi
    print("✓ cdsapi package is installed")
    
    try:
        c = cdsapi.Client()
        print("✓ CDS API client initialized successfully")
        print("✓ Credentials are valid")
        print("")
        print("✅ CDS API is ready to use!")
        sys.exit(0)
    except Exception as e:
        print(f"❌ Failed to initialize CDS API client: {e}")
        print("")
        print("Possible issues:")
        print("1. Invalid UID or API key")
        print("2. Network connectivity issues")
        print("3. CDS API service is down")
        print("")
        print("Please verify your credentials at:")
        print("https://cds.climate.copernicus.eu/api-how-to")
        sys.exit(1)
except ImportError:
    print("❌ cdsapi package is not installed")
    print("")
    print("Please install it with:")
    print("  pip install cdsapi")
    sys.exit(1)
PYEOF

TEST_RESULT=$?

if [ $TEST_RESULT -eq 0 ]; then
    echo ""
    echo "=============================================="
    echo "Next Steps:"
    echo "=============================================="
    echo ""
    echo "1. Ensure you have accepted the ERA5-Land Terms and Conditions:"
    echo "   https://cds.climate.copernicus.eu/cdsapp#!/dataset/reanalysis-era5-land-monthly-means"
    echo ""
    echo "2. Mount .cdsapirc in Docker container by adding to docker-compose.yml:"
    echo "   volumes:"
    echo "     - ~/.cdsapirc:/home/airflow/.cdsapirc:ro"
    echo ""
    echo "3. Rebuild and restart your containers:"
    echo "   docker-compose down"
    echo "   docker-compose up -d"
    echo ""
    echo "4. Trigger the climate data DAG in Airflow:"
    echo "   http://localhost:8080"
    echo "   DAG: load_climate_raw_data"
    echo ""
else
    echo ""
    echo "❌ CDS API test failed. Please fix the issues above and try again."
    exit 1
fi
