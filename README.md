# epic_cosmos

This is a modified version of a Data Collection Framework project, initialized with `dcf::dcf_init`.

To rebuild the entire data package, run dcf::dcf_process('./data/bundle_cosmos'). Note that within this file, there are logical switches for running individual parts (e.g., maybe you only want to run respiratory, which is updated more frequently)

**To just update the respiratory bundle:**
1) Replace the files in data/cosmos_resp_infections/raw/staging_resp_infections_wide
2) If running this on a new computer, run usethis::edit_r_environ(), and add EPIC_XLSX_PASSWORD=******* as an environment variable
3) Run dcf::dcf_process('cosmos_resp_infections') from the root directory
