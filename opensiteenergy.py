import logging
from opensite.app.opensite import OpenSiteApplication

def main():

    # Run OpenSite application
    app = OpenSiteApplication(logging.INFO)
    app.run()

if __name__ == "__main__":
    main()