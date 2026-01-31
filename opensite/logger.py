import logging
import sys



def LogOutOfMemoryAndQuit():
    """
    Logs out of memory message and quits
    """

    LogError("")
    LogError("*** Build failure likely due to lack of memory ***")
    LogError("If running local install, increase swap disk size to > 10Gb")
    LogError("If running Docker install, increase Docker swap size by editing Docker config file:")
    LogError("1. Edit Docker config file - for locations see https://docs.docker.com/desktop/settings-and-maintenance/settings/")
    LogError("2. Modify 'SwapMiB' and set to 10000")
    LogError("3. Fully quit and restart Docker for new 'SwapMiB' setting to take effect")
    LogError("4. Rerun ./build-docker.sh")

    exit()

