-- First, create a network rule to allow external API calls
CREATE OR REPLACE NETWORK RULE pi_qualytics_api_rule
  MODE = EGRESS
  TYPE = HOST_PORT
  VALUE_LIST = ('your-app-domain.vercel.app:443', 'localhost:3000');

-- Create a secret for the API (if needed for authentication)
CREATE OR REPLACE SECRET pi_qualytics_api_secret
  TYPE = GENERIC_STRING
  SECRET_STRING = 'your-api-key-if-needed';

-- Create external access integration
CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION pi_qualytics_integration
  ALLOWED_NETWORK_RULES = (pi_qualytics_api_rule)
  ALLOWED_AUTHENTICATION_SECRETS = (pi_qualytics_api_secret)
  ENABLED = TRUE;