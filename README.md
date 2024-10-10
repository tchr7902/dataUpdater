# Data Updater

SnipeIT Data Visualization Tool

## Description

This script will first connect to an instance of a SnipeIT MySQL database. Then, it will query relevant data for reporting, and populate a Google Sheet with this data. The purpose of this is to help our team visualize various aspects of our hardware, such as cost, maintenance costs, which store is the most maintenance intensive, etc.

## Getting Started

Create a .env file and create your own credentials to access the database.\
Run 'python update_google_sheet.py' and sit back and relax!

### Dependencies

gpsread, oauth, mysql.connector, dotenv

## Author

tchr7902\Trevor Christensen\[Gmail](trevorchristensen3405@gmail.com)

## License

This project is licensed under the MIT License - see the LICENSE.md file for details
