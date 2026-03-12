# epic_cosmos

This is a modified version of a Data Collection Framework project, initialized with `dcf::dcf_init`.

To rebuild the entire data package, run dcf::dcf_process('./data/bundle_cosmos'). Note that within this file, there are logical switches for running individual parts (e.g., maybe you only want to run respiratory, which is updated more frequently)

To just update the respiratory bundle:
1) Replace the files in raw/staging_resp_infections_wide
2) dcf::dcf_process('./data/cosmos_resp_infections')