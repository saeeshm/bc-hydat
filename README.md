# The BC-Hydrometric Data Download Program
This is a program that uses a Selenium server to navigate through the Government of Canada Hydrometric Database and downloads hydrometric data for all active stations in BC for the last 18 months (starting from the day the program is run). The purpose of the program is obtain the most recent real-time hydrostatic data that has not yet been verified and thus is not present in the published Hydat database. For obtaining verified data, please see the [BC Hydat database](https://www.canada.ca/en/environment-climate-change/services/water-overview/quantity/monitoring/survey/data-products-services/national-archive-hydat.html).

The program downloads available real-time data for each station and exports them into csv files, with a distinct dataset for each parameter (such as *water level*, *discharge*, *battery voltage*, *air temperature* and so on).

**IMPORTANT**: The program has only been developed and tested in a windows operating environment and thus may not operate correctly on UNIX systems (likely due to differences in how file paths are presented). A UNIX compatible version is still in development and should be available soon.

## Running the Program: The TL-DR
The are all the basic instructions needed to get the program working. More detail as well as examples can be found in the sections below.

1. **Preserve the directory structure** of the BC_hydrometric folder, ensuring at minimum that all of the scripts as well as a folder titled ```data``` are present.

2. Whenever the folder is moved to a new location, open the ```chromeOptions.R``` and ensure that the download directory points to the ```zip``` sub-folder using an **absolute path**

3. Ensure that the directory containing the R executable is on the system's PATH Environment variable, done as follows on [Windows](https://support.shotgunsoftware.com/hc/en-us/articles/114094235653-Setting-global-environment-variables-on-Windows). The R executable is by default located in the ```Program Files``` directory with a path as follows:
```
C:\Program Files\R\R-4.0.1\bin
```

4. Run the ```scrapeHydatData.bat``` file. To get data for the full set of stations (about 480) it may take upto 4 hours. The program will run in the background, as long as the **chrome browser window opened by selenium is not closed**. Once the program has finished, you can close the command line interface by clicking any button. If you prefer, you can also open the ```scrapeHydatData.R``` file in RStudio and run it from there.

5. It is recommended that the user rename or move the ```output``` subdirectory (located within the ```data``` folder) once the program finished (ideally with the data of the download) since this directory is overwritten every time the program is run afresh (erasing any previous files present within it).

## Running the Program: The Long version
Ensure that steps 1, 2 and 3 of the preceding section are complete before proceeding with running the program. Also ensure that step 5 is completed after the program is run.

### Default settings
By default, the program uses a chrome browser version ```83.0.4103.39```. If there are issues with running this browser with your system, you can change the options to run a different version. At the moment only the chrome browser is support. You should not need the chrome browser yourself, as a usable driver is automatically installed when you install ```RSelenium``` but in case there are problems with starting the driver you may need to install the browser yourself.

Secondly, in the interest of conserving storage space, the program's default behavior is to not store the individual csv files for each station. Instead it joins the data for all stations into a number of master tables organized by feature type (stored in the ```output``` folder). IF you prefer, you can ask that these individual files are stored.

To see how these options can be specified, please see the ***Running the Program Using Custom Settings*** section below.

### Running the Program Using Default Settings
For users unfamiliar with the RStudio interface or just looking for the easiest option, the recommended method is to execute the ```scrapeHydatData.bat``` file stored in the directory. This will open command line R Session to execute the program. The directory containing the R executable **MUST** be on the system's PATH Environment variable (see instructions above) for the program to be run this way.

For users who prefer not to work with the command line, the program can also be run by opening the scrapeHydatData.R script in RStudio and running it there. This method will work even if the R executable is not on the system's PATH variable.

The program will automatically install all required dependencies and call supporting scripts. The time taken will vary depending on how many stations are being scraped and your system's capabilities, but can go up to multiple hours. Once started, it can be left idle until all downloading is complete as no further user input is required.

### Running the Program using Custom Settings
In order to run with custom settings, the directory containing the R executable **MUST** be on the system's PATH Environment variable (see instructions above). The following steps should work for both Windows and UNIX systems.

Open a command terminal and navigate to the directory where the program is stored. You can use the ```cd``` command along with the full file path to the directory. For example:

```
cd C:\Users\<username>\GWS_Projects\BC_hydrometric
```
Once in this directory, you can run R scripts located here simply by typing ```RScript``` before the script's name. For instance:
```
Rscript example.R
```
Run this way, you can provide the program with arguments to alter its default settings. To see descriptions for the arguments that the program can take, run the following command.
```
Rscript --vanilla test.R --help
```
The syntax of option passing is similar to option passing in the shell. For instance, to ask the program to keep the individual downloaded csvs instead of deleting them, the script would be called as follows.
```
Rscript --vanilla scrapeHydatData.R -c TRUE
```
The following example uses all of the available options. It uses default values, so the program run using this command will in fact the same as if it were just being run using the ```.bat``` file or through RStudio.
```
Rscript --vanilla scrapeHydatData.R -k FALSE -v 86.0.4240.22 -s Inf -m 18
```

#### A note on the use of --vanilla
The ```--vanilla``` argument is passed to prevent R from saving all the objects and variables created during the program's operation, as these take up unnecessary space and are generally not needed once the downloaded data files are exported. If however the user would like to keep these objects, simply drop the vanilla option.

## The Program's Operation
The program opens an automated browser window using a Selenium server, which handles the data download process. A browser window pops up on first being opened, but can be minimized and left alone while the program runs. It is important that the browser window not be closed else the execution will stop.

Status updates will continuously be printed in the RStudio console or the command window, depending on how the program has been run, indicating to the user how much of the total data has been successfully downloaded.

### Outputs
The program downloads and organizes all pulled data into output datasets, all of which are stored in the ```output``` subdirectory under the ```data``` folder. A new table is generated for each different information type and any station that gathers this sort of data is added to the respective table. The tables contain **daily mean** values for the parameter of interest.

In addition to having a table for each data type, two more tables are generated - ```extraction_summary.csv``` and ```problem_stations.csv```. The first contains a summary of all the data that were successfully extracted for each station (identified by their station ID), while the second contains the IDs of all the stations where the download failed, as well as a general description of the issue that occurred (some wells are recorded twice, due to having more than 1 issue which prevented the download). The ```problem_stations.csv``` provides a good reference for all the wells that may either need to be handled manually or have unresolvable errors.

The most common reasons for the failure to download data include data not being available for the time-range requested, the download link being broken and the station having been mis-labelled as active when it was in fact inactive.

## Known Issues

#### 1. The Unzip function:
The data for each station are downloaded as a zip file, which must be extracted to get the data as a csv. However, there is a problem within the R unzip function (for both the inbuilt unzip function and the function in the ```zip``` library) on Windows systems, where **it fails to unzip if the file path to the source file or the destination directory is too long (more than about 90 characters)**. As a result, if the program directory is stored very deep in a complex folder, it may fail to download any data.

The only workaround is to try to ensure that the program folder is located relatively close to the root (near the C drive, in the case of Windows) so that the file paths provided to it are relatively simple and don't cause errors.
