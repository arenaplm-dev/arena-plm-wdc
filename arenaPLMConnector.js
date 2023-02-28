///////////////////////////////////////////////////////////////////////////////////////////
// Name: Arena PLM WDC                                                                   //
// Description: A Tableau Web Data Connector for connecting to Arena REST APIs           //
//              and downloading data from the Data Extract service                       //
// Version 1.0                                                                           //
// Credit: Enhancement from Tableau Employee WDC Example for CSV data parsing            //
// GitHub URL: https://github.com/KeshiaRose/Basic-CSV-WDC/blob/master/public/script.js  //
///////////////////////////////////////////////////////////////////////////////////////////

// init global variables
// let savedArenaSessionID; 
// let savedArenaDESetupGUID;
// let savedArenaDELatestRunGUID;
// let savedArenaDELatestRunFileGUID;
// let savedArenaDELatestRunFileContents;
let myConnector = tableau.makeConnector();

// Populate inputs entered from Interactive Phase (HTML page)
myConnector.init = function(initCallback) {
    if (
        tableau.phase == tableau.phaseEnum.interactivePhase &&
        tableau.connectionData.length > 0
    ) {
        const conData = JSON.parse(tableau.connectionData);
        $("#arena_api_url").val(conData.arena_api_url || "");
        $("#arena_email").val(tableau.arena_email || "");
        $("#arena_password").val(tableau.arena_password || "");
        $("#arena_workspace_id").val(tableau.arena_workspace_id || "");
    }
    initCallback();
};

// Fetch the Arena data and create the schema
myConnector.getSchema = async function(schemaCallback) {
    let conData = JSON.parse(tableau.connectionData);
    let arenaUrl = conData.arenaUrl;
    let arenaEmail = conData.arenaEmail;
    let arenaPassword = tableau.password;
    let arenaWorkspaceID = conData.arenaWorkspaceID;

    // get Arena Login Session ID
    let arenaSessionID = //savedArenaSessionID || 
    (await _getArenaLoginSessionId(arenaUrl, arenaEmail, arenaPassword, arenaWorkspaceID ));

    // get Arena Data Extract Setup GUID
    let arenaDESetupGUID = //savedArenaDESetupGUID || 
    (await _getDataExtractSetupGUID( arenaUrl, arenaSessionID ));

    // get Arena Data Extract Latest Run GUID
    let arenaDELatestRunGUID = //savedArenaDELatestRunGUID || 
    (await _getDataExtractLatestRunGUID( arenaUrl, arenaSessionID, arenaDESetupGUID ));

    // get Arena Data Extract Latest Run File GUID
    let arenaDELatestRunFileGUID = //savedArenaDELatestRunFileGUID || 
    (await _getDataExtractLatestRunFileGUID( arenaUrl, arenaSessionID, arenaDESetupGUID, arenaDELatestRunGUID ));

    // get Arena Data Extract Latest Run File Contents
    let arenaDELatestRunFileContents = //savedArenaDELatestRunFileContents || 
    (await _getDataExtractLatestRunFileContents( arenaUrl, arenaSessionID, arenaDESetupGUID, arenaDELatestRunGUID, arenaDELatestRunFileGUID ));

    // convert to ZIP and unpack CSV files
    const zipFile = new File([arenaDELatestRunFileContents], 'arena_data.zip', {
        type: arenaDELatestRunFileContents.type,
    });

    let ArenaDETableSchema = [];
    var jsZip = new JSZip();
    let zip = await jsZip.loadAsync(zipFile);
    
    for(filename in zip.files) {
        // only parse CSV files in the ZIP file
        if (filename.endsWith('.csv')){
            let csvDETableName = await _getArenaTableName( filename );
            
            // read the CSV file contents
            let csvFile = await zip.files[filename].async('string');

            // parse out special characters that are included currently in data extract files
            let parsedFileData = csvFile.replaceAll("\\\\", "\\").replaceAll("\\\n",". ").replaceAll("\"\"", "\"");

            // define tables and column schema for each data extract table
            let cols = [];
            let parsed = await _parse(parsedFileData, false);
            let headers = _determineTypes(parsed);
            
            // console.log("Starting Table: " + csvDETableName);
            for (let field in headers) {
                // console.log("Col: " + headers[field].alias + " | " + headers[field].dataType);

                cols.push({
                    id: field,
                    alias: headers[field].alias,
                    dataType: headers[field].dataType
                });
            }

            // needs to be multiple tables in schema
            ArenaDETableSchema.push({
                id: csvDETableName,
                alias: csvDETableName,
                columns: cols
            });            
        }
    };  

    tableau.connectionData = JSON.stringify({
        arenaUrl: arenaUrl,
        arenaSessionID: arenaSessionID,
        arenaDESetupGUID: arenaDESetupGUID,
        arenaDELatestRunGUID: arenaDELatestRunGUID,
        arenaDELatestRunFileGUID: arenaDELatestRunFileGUID
    });
      
    // console.log("ArenaDETableSchema: " + JSON.stringify(ArenaDETableSchema));
    schemaCallback(ArenaDETableSchema);
};

// Fetch the data and parse
myConnector.getData = async function(table, doneCallback) {
    // console.log("Getting getData");

    let conData = JSON.parse(tableau.connectionData);
    let arenaUrl = conData.arenaUrl;
    let arenaSessionID = conData.arenaSessionID;
    let arenaDESetupGUID = conData.arenaDESetupGUID;
    let arenaDELatestRunGUID = conData.arenaDELatestRunGUID;
    let arenaDELatestRunFileGUID = conData.arenaDELatestRunFileGUID;


    // // get Arena Login Session ID
    // let arenaSessionID = // savedArenaSessionID || 
    // (await _getArenaLoginSessionId( arenaUrl, arenaEmail, arenaPassword, arenaWorkspaceID ));

    // // get Arena Data Extract Setup GUID
    // let arenaDESetupGUID = // savedArenaDESetupGUID || 
    // (await _getDataExtractSetupGUID( arenaUrl, arenaSessionID ));

    // // get Arena Data Extract Latest Run GUID
    // let arenaDELatestRunGUID = // savedArenaDELatestRunGUID || 
    // (await _getDataExtractLatestRunGUID( arenaUrl, arenaSessionID, arenaDESetupGUID ));

    // // get Arena Data Extract Latest Run File GUID
    // let arenaDELatestRunFileGUID = // savedArenaDELatestRunFileGUID || 
    // (await _getDataExtractLatestRunFileGUID( arenaUrl, arenaSessionID, arenaDESetupGUID, arenaDELatestRunGUID ));

    // get Arena Data Extract Latest Run File Contents
    let arenaDELatestRunFileContents = // savedArenaDELatestRunFileContents; //|| 
     (await _getDataExtractLatestRunFileContents( arenaUrl, arenaSessionID, arenaDESetupGUID, arenaDELatestRunGUID, arenaDELatestRunFileGUID ));

    // convert to ZIP and unpack CSV files
    const zipFile = new File([arenaDELatestRunFileContents], 'arena_data.zip', {
        type: arenaDELatestRunFileContents.type,
    });

    var jsZip = new JSZip();
    let zip = await jsZip.loadAsync(zipFile);

    for(filename in zip.files) {
        if(filename.endsWith('.csv')){
            let csvDETableName = await _getArenaTableName(filename);
            
            if(table.tableInfo.id == csvDETableName) {
                // read the CSV file contents
                let csvFile = await zip.files[filename].async('string');

                // parse out special characters that are included currently in data extract files
                let parsedFileData = csvFile.replaceAll("\\\\", "\\").replaceAll("\\\n",". ").replaceAll("\"\"", "\"");
                let csvData = await _parse(parsedFileData, true);

                // Parse data for Tableau load
                let row_index = 0;
                let size = 10000;
                while (row_index < csvData.length) {
                    table.appendRows(csvData.slice(row_index, size + row_index));
                    row_index += size;
                    tableau.reportProgress("Getting row: " + row_index);
                }

                doneCallback();
            }
        }
    };    
};

tableau.connectionName = "Arena PLM Connector";
tableau.registerConnector(myConnector);
window._tableau.triggerInitialization && window._tableau.triggerInitialization(); // Make sure WDC is initialized properly

// Grabs wanted fields and submits configuration to Tableau
async function _submitDataToTableau() {
    let arenaUrl = $("#arena_api_url").val().trim();
    let arenaEmail = $("#arena_email").val().trim();
    let arenaPassword = $("#arena_password").val();
    let arenaWorkspaceID = $("#arena_workspace_id").val().trim();

    if (!arenaEmail) return _error("No Arena Email entered.");
    if (!arenaPassword) return _error("No Arena Password entered.");
    if (!arenaWorkspaceID) return _error("No Arena Workspace ID entered.");

//     const urlRegex = /^(http:\/\/www\.|https:\/\/www\.|http:\/\/|https:\/\/|ftp:\/\/)?[a-z0-9]+([\-\.]{1}[a-z0-9]+)*\.[a-z]{2,5}(:[0-9]{1,5})?(\/.*)?$/gm;
//     const result = arenaUrl.match(urlRegex);
   
//     if (result === null) {
//         _error("WARNING: URL may not be valid...");
//      await new Promise(resolve => setTimeout(resolve, 2000));
//     }

    tableau.connectionData = JSON.stringify({
        arenaUrl,
        arenaEmail,
        arenaWorkspaceID
    });
    
    tableau.password = arenaPassword;
    tableau.submit();
}

async function _getArenaTableName( filename ){
    // parse full file name and get the Data Extract Table name only
    let csvDETableName;
    if(filename.includes("Requests_Summary")) csvDETableName = "Requests_Summary";
    else if(filename.includes("Requests_Custom_Attributes")) csvDETableName = "Requests_Custom_Attributes";
    else if(filename.includes("Requests_Lifecycle_History")) csvDETableName = "Requests_Lifecycle_History";
    else if(filename.includes("Requests_Items")) csvDETableName = "Requests_Items";
    else if(filename.includes("Requests_Participants")) csvDETableName = "Requests_Participants";
    else if(filename.includes("Requests_Issues")) csvDETableName = "Requests_Issues";
    else if(filename.includes("Changes_Summary")) csvDETableName = "Changes_Summary";
    else if(filename.includes("Changes_Custom_Attributes")) csvDETableName = "Changes_Custom_Attributes";
    else if(filename.includes("Changes_Decision_History")) csvDETableName = "Changes_Decision_History";
    else if(filename.includes("Changes_Lifecycle_History")) csvDETableName = "Changes_Lifecycle_History";
    else if(filename.includes("Changes_Items")) csvDETableName = "Changes_Items";
    else if(filename.includes("Changes_Requests")) csvDETableName = "Changes_Requests";
    else if(filename.includes("Changes_Implementation_Notes")) csvDETableName = "Changes_Implementation_Notes";
    else if(filename.includes("Changes_Item_Inventory_Disposition")) csvDETableName = "Changes_Item_Inventory_Disposition";
    else if(filename.includes("Suppliers_Summary")) csvDETableName = "Suppliers_Summary";
    else if(filename.includes("Suppliers_Custom_Attributes")) csvDETableName = "Suppliers_Custom_Attributes";
    else if(filename.includes("Supplier_Items_Summary")) csvDETableName = "Supplier_Items_Summary";
    else if(filename.includes("Supplier_Items_Custom_Attributes")) csvDETableName = "Supplier_Items_Custom_Attributes";
    else if(filename.includes("Supplier_Items_Compliance")) csvDETableName = "Supplier_Items_Compliance";
    else if(filename.includes("Projects_Summary")) csvDETableName = "Projects_Summary";
    else if(filename.includes("Projects_Schedule")) csvDETableName = "Projects_Schedule";
    else if(filename.includes("Projects_Referenced_Items")) csvDETableName = "Projects_Referenced_Items";
    else if(filename.includes("Projects_Referenced_Changes")) csvDETableName = "Projects_Referenced_Changes";
    else if(filename.includes("Projects_Referenced_Requests")) csvDETableName = "Projects_Referenced_Requests";
    else if(filename.includes("Projects_Referenced_Projects")) csvDETableName = "Projects_Referenced_Projects";
    else if(filename.includes("Projects_Referenced_Quality")) csvDETableName = "Projects_Referenced_Quality";
    else if(filename.includes("Projects_Referenced_URL")) csvDETableName = "Projects_Referenced_URL";
    else if(filename.includes("Quality_Summary")) csvDETableName = "Quality_Summary";
    else if(filename.includes("Quality_Custom_Attributes")) csvDETableName = "Quality_Custom_Attributes";
    else if(filename.includes("Quality_Details")) csvDETableName = "Quality_Details";
    else if(filename.includes("Quality_Affected_Items")) csvDETableName = "Quality_Affected_Items";
    else if(filename.includes("Quality_Affected_Changes")) csvDETableName = "Quality_Affected_Changes";
    else if(filename.includes("Quality_Affected_Requests")) csvDETableName = "Quality_Affected_Requests";
    else if(filename.includes("Quality_Affected_Projects")) csvDETableName = "Quality_Affected_Projects";
    else if(filename.includes("Quality_Affected_Quality")) csvDETableName = "Quality_Affected_Quality";
    else if(filename.includes("Quality_Affected_URL")) csvDETableName = "Quality_Affected_URL";
    else if(filename.includes("Quality_History")) csvDETableName = "Quality_History";
    else if(filename.includes("Quality_Decisions")) csvDETableName = "Quality_Decisions";
    else if(filename.includes("Quality_Affected_Suppliers")) csvDETableName = "Quality_Affected_Suppliers";
    else if(filename.includes("Quality_Affected_Supplier_Items")) csvDETableName = "Quality_Affected_Supplier_Items";
    else if(filename.includes("Training_Summary")) csvDETableName = "Training_Summary";
    else if(filename.includes("Training_Items")) csvDETableName = "Training_Items";
    else if(filename.includes("Training_Users")) csvDETableName = "Training_Users";
    else if(filename.includes("Training_Quality")) csvDETableName = "Training_Quality";
    else if(filename.includes("Training_Records")) csvDETableName = "Training_Records";
    else if(filename.includes("Training_History")) csvDETableName = "Training_History";
    else if(filename.includes("Items_Summary")) csvDETableName = "Items_Summary";
    else if(filename.includes("Items_Custom_Attributes")) csvDETableName = "Items_Custom_Attributes";
    else if(filename.includes("Items_BOM_Custom_Attributes")) csvDETableName = "Items_BOM_Custom_Attributes";
    else if(filename.includes("Items_BOM_Substitutes")) csvDETableName = "Items_BOM_Substitutes";
    else if(filename.includes("Items_BOM")) csvDETableName = "Items_BOM";
    else if(filename.includes("Items_Sourcing")) csvDETableName = "Items_Sourcing";
    else if(filename.includes("Items_Compliance")) csvDETableName = "Items_Compliance";
    else if(filename.includes("Items_Non_Rev_Controlled_Custom_Attributes")) csvDETableName = "Items_Non_Rev_Controlled_Custom_Attributes";
    return csvDETableName;
}

async function _getArenaLoginSessionId( arenaUrl, arenaEmail, arenaPassword, arenaWorkspaceID ) {
    let result;

    try {
        let options = {
            method: "POST",
            headers: {
                "Content-Type": "application/json"
            },
            body: JSON.stringify({
                "email": arenaEmail,
                "password": arenaPassword,
                "workspaceId": parseInt(arenaWorkspaceID)
            })
        }; 

        const response = await fetch(arenaUrl + "/login", options);
        result = await response.json();
    } catch (error) {
        console.log("Arena Error: "+ error);

        if (tableau.phase !== "interactive") {
            tableau.abortWithError(error);
            } else {
                _error(error);
                console.log("")
            }
        }

    if (!result || result.error) {
        console.log("Arena Error: "+ result.error);
        if (tableau.phase !== "interactive") {
            console.error(result.error);
            tableau.abortWithError(result.error);
        } else {
          _error(result.error);
        }
        return;
    }

    // savedArenaSessionID = result.arenaSessionId;
    // return savedArenaSessionID;
    return result.arenaSessionId;
}

// Get Data Extract Setup GUID 
async function _getDataExtractSetupGUID( arenaUrl, arenaSessionID ) {
    let result; 

    try {
        let options = {
            method: "GET",
            headers: {
                "arena_session_id": arenaSessionID,
                "Content-Type": "application/json"
            },
        }; 

        const response = await fetch(arenaUrl + "/extracts", options);
        result = await response.json();

    } catch (error) {
        console.log("Arena Error: "+ error);

        if (tableau.phase !== "interactive") {
            tableau.abortWithError(error);
            } else {
                _error(error);
                console.log("")
            }
        }

    if (!result || result.error) {
        console.log("Arena Error: "+ result.error);
        if (tableau.phase !== "interactive") {
            console.error(result.error);
            tableau.abortWithError(result.error);
        } else {
          _error(result.error);
        }
        return;
    }

    // savedArenaDESetupGUID = result.results[0].guid;
    // return savedArenaDESetupGUID;
    return result.results[0].guid;
}

// Get Data Extract Latest Run GUID 
async function _getDataExtractLatestRunGUID( arenaUrl, arenaSessionID, arenaDESetupGUID ) {
    let result; 

    try {
        let options = {
            method: "GET",
            headers: {
                "arena_session_id": arenaSessionID,
                "Content-Type": "application/json"
            },
        }; 

        const response = await fetch(arenaUrl + "/extracts/" + arenaDESetupGUID + "/runs/latestCompleted", options);
        result = await response.json();

    } catch (error) {
        console.log("Arena Error: "+ error);

        if (tableau.phase !== "interactive") {
            tableau.abortWithError(error);
            } else {
                _error(error);
                console.log("")
            }
        }

    if (!result || result.error) {
        console.log("Arena Error: "+ result.error);
        if (tableau.phase !== "interactive") {
            console.error(result.error);
            tableau.abortWithError(result.error);
        } else {
          _error(result.error);
        }
        return;
    }

    // savedArenaDELatestRunGUID = result.guid;
    // return savedArenaDELatestRunGUID
    return result.guid;
}

// Get Data Extract Latest Run File GUID 
async function _getDataExtractLatestRunFileGUID( arenaUrl, arenaSessionID, arenaDESetupGUID, arenaDELatestRunGUID ) {
    let result; 

    try {
        let options = {
            method: "GET",
            headers: {
                "arena_session_id": arenaSessionID,
                "Content-Type": "application/json"
            },
        }; 

        const response = await fetch(arenaUrl + "/extracts/" + arenaDESetupGUID + "/runs/" + arenaDELatestRunGUID + "/files", options);
        result = await response.json();

    } catch (error) {
        console.log("Arena Error: "+ error);

        if (tableau.phase !== "interactive") {
            tableau.abortWithError(error);
            } else {
                _error(error);
                console.log("")
            }
        }

    if (!result || result.error) {
        console.log("Arena Error: "+ result.error);
        if (tableau.phase !== "interactive") {
            console.error(result.error);
            tableau.abortWithError(result.error);
        } else {
           _error(result.error);
        }
        return;
    }

    // savedArenaDELatestRunFileGUID = result.results[0].guid;
    // return savedArenaDELatestRunFileGUID;
    return result.results[0].guid;
}

// Get Data Extract Latest Run File Contents 
async function _getDataExtractLatestRunFileContents( arenaUrl, arenaSessionID, arenaDESetupGUID, arenaDELatestRunGUID, arenaDELatestRunFileGUID ) {
    let result; 

    try {
        let options = {
            method: "GET",
            headers: {
                "arena_session_id": arenaSessionID,
                "Content-Type": "application/json"
            },
        }; 

        const response = await fetch(arenaUrl + "/extracts/" + arenaDESetupGUID + "/runs/" + arenaDELatestRunGUID + "/files/" + arenaDELatestRunFileGUID + "/content", options);
        result = await response.blob();

    } catch (error) {
        console.log("Arena Error: "+ error);

        if (tableau.phase !== "interactive") {
            tableau.abortWithError(error);
            } else {
                _error(error);
                console.log("")
            }
        }

    if (!result || result.error) {
        console.log("Arena Error: "+ result.error);
        if (tableau.phase !== "interactive") {
            console.error(result.error);
            tableau.abortWithError(result.error);
        } else {
           _error(result.error);
        }
        return;
    }

    // savedArenaDELatestRunFileContents = result;
    // return savedArenaDELatestRunFileContents;
    return result;
}

// Sanitizes headers so they work in Tableau without duplicates
function _sanitizeKeys(fields) {
  let headers = {};
  for (let field of fields) {
    let newKey = field.replace(/[()\/]/g, "").replace(/[^A-Za-z0-9_]/g, "_").replaceAll("__","_");
    let safeToAdd = false;

    do {
      if (Object.keys(headers).includes(newKey)) {
        newKey += "_copy";
      } else {
        safeToAdd = true;
      }
    } while (!safeToAdd);

    headers[newKey] = { alias: field };
  }
  return headers;
}

// Parses csv to array of arrays
async function _parse(csv, header) {
  const lines = Papa.parse(csv, {
    delimiter:",",
    header: header,
    newline: "\n",
    dynamicTyping: true,
    skipEmptyLines: header,
    transformHeader:function(h) {
        return h.replaceAll(" ", "_").replace(/[()\/]/g, "");
      }
  }).data;
  return lines;
}

// Determines column data types based on first 100 rows
function _determineTypes(lines) {
  let fields = lines.shift();
  let testLines = lines.slice(0, 100);
  let headers = _sanitizeKeys(fields);
  let headerKeys = Object.keys(headers);

  let counts = testLines.map(line => line.length);
  let lineLength = counts.reduce((m, c) =>
    counts.filter(v => v === c).length > m ? c : m
  );

  for (let line of testLines) {
    if (line.length === lineLength) {
      for (let index in headerKeys) {
        let header = headers[headerKeys[index]];
        let value = line[index];

        if (
            value === "" ||
            value === '""' ||
            value === "null" ||
            value === null
        ) {
            header.null = header.null ? header.null + 1 : 1;
        } else if (
            value === "TRUE" ||
            value === "true" ||
            value === true ||
            value === "FALSE" ||
            value === "false" ||
            value === false
        ) {
            header.bool = header.bool ? header.bool + 1 : 1;
        } else if (typeof value === "object") {           
            if (!isNaN(Date.parse(value))) {
                header.date = header.date ? header.date + 1 : 1;
            } else {
                header.string = header.string ? header.string + 1 : 1;
            }
        } else if (!isNaN(value)) {
            if (parseInt(value) == value) {
            header.int = header.int ? header.int + 1 : 1;
            } else {
            header.float = header.float ? header.float + 1 : 1;
            }
        } else {
            header.string = header.string ? header.string + 1 : 1;
        }
      }
    } else {
      console.log("Row ommited due to mismatched length.", line);
    }
  }

  for (let field in headers) {
    // strings
    if (headers[field].string) {
        if(field.includes("Date")) {
            headers[field].dataType = "datetime";
        } else {
            headers[field].dataType = "string";
        }
        continue;
    }
    // datetimes
    if (headers[field].date) {
        headers[field].dataType = "datetime";
        continue;
      }
    // nulls
    if (Object.keys(headers[field]).length === 1 && headers[field].null) {
        if(field.includes("Date")) {
            headers[field].dataType = "datetime";
        } else {
            headers[field].dataType = "string";
        }
      continue;
    }
    // floats
    if (headers[field].float) {
        headers[field].dataType = "float";
        continue;
    }
    // integers
    if (headers[field].int) {
        headers[field].dataType = "int";
        continue;
    }
    // booleans
    if (headers[field].bool) {
        headers[field].dataType = "bool";
        continue;
    }
    
    if(field.includes("Date")) {
        headers[field].dataType = "datetime";
    } else {
        headers[field].dataType = "string";
    }
  }

  return headers;
}

// Shows error message below submit button
function _error(message) {
  $(".error")
    .fadeIn("fast")
    .delay(3000)
    .fadeOut("slow");
  $(".error").html(message);
  $("html, body").animate({ scrollTop: $(document).height() }, "fast");
}
