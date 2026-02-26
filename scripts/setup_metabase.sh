#!/bin/bash
# Script to automatically configure MongoDB connection in Metabase

echo "Waiting for Metabase to be ready..."
until curl -s http://localhost:3000/api/health | grep -q "ok"; do
    echo "Metabase not ready yet, waiting..."
    sleep 5
done

echo "Metabase is ready!"

# Get setup token or session
SETUP_RESPONSE=$(curl -s http://localhost:3000/api/session/properties)
SETUP_TOKEN=$(echo "$SETUP_RESPONSE" | grep -o '"setup-token":"[^"]*"' | cut -d'"' -f4)

if [ ! -z "$SETUP_TOKEN" ]; then
    echo "Metabase needs initial setup..."
    
    # Create admin user and add MongoDB connection in one setup call
    curl -X POST http://localhost:3000/api/setup \
        -H "Content-Type: application/json" \
        -d '{
            "token": "'$SETUP_TOKEN'",
            "user": {
                "first_name": "Admin",
                "last_name": "User",
                "email": "admin@example.com",
                "password": "Climate2026!",
                "site_name": "Climate Analysis"
            },
            "database": {
                "engine": "mongo",
                "name": "Climate Analysis MongoDB",
                "details": {
                    "host": "mongodb",
                    "port": 27017,
                    "dbname": "analytical",
                    "user": "admin",
                    "pass": "admin",
                    "authdb": "admin",
                    "ssl": false
                },
                "is_full_sync": true,
                "is_on_demand": false,
                "schedules": {}
            },
            "prefs": {
                "site_name": "Climate Analysis",
                "allow_tracking": false
            }
        }'
    
    echo ""
    echo "✓ Metabase setup completed!"
    echo "Login at http://localhost:3000 with:"
    echo "  Email: admin@example.com"
    echo "  Password: Climate2026!"
else
    echo "Metabase already set up. Adding MongoDB connection..."
    
    # Login to get session token
    SESSION_RESPONSE=$(curl -X POST http://localhost:3000/api/session \
        -H "Content-Type: application/json" \
        -d '{"username": "admin@example.com", "password": "Climate2026!"}' \
        -s)
    SESSION=$(echo "$SESSION_RESPONSE" | grep -o '"id":"[^"]*"' | cut -d'"' -f4)
    
    if [ ! -z "$SESSION" ]; then
        # Add MongoDB database
        curl -X POST http://localhost:3000/api/database \
            -H "Content-Type: application/json" \
            -H "X-Metabase-Session: $SESSION" \
            -d '{
                "engine": "mongo",
                "name": "Climate Analysis MongoDB",
                "details": {
                    "host": "mongodb",
                    "port": 27017,
                    "dbname": "analytical",
                    "user": "admin",
                    "pass": "admin",
                    "authdb": "admin",
                    "ssl": false
                },
                "is_full_sync": true,
                "is_on_demand": false,
                "schedules": {}
            }'
        
        echo ""
        echo "✓ MongoDB connection added to Metabase!"
    else
        echo "Could not login to Metabase. Please add MongoDB connection manually."
        echo "Use the credentials in docs/METABASE_SETUP.md"
    fi
fi

echo ""
echo "Access Metabase at: http://localhost:3000"
