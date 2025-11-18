#https://github.com/DISSC-yale/dcf
#remotes::install_github("dissc-yale/dcf")

library(dcf)
library(tidyverse)

dcf_build()

##RUN ONCE from parent directory (not within an existing project) 
# dcf_init('epic_cosmos')
#########

###########
#Add new sources
###########

#dcf::dcf_add_source("cosmos")



###########################
#Process individual sources
###########################
# dcf_process("cosmos")


#check structure of the files before merging
#dcf::dcf_check('cosmos')

## Add bundles
### dcf::dcf_add_bundle("bundle_respiratory")
### dcf::dcf_add_bundle("bundle_chronic_diseases")
### dcf::dcf_add_bundle("bundle_childhood_immunizations")
### dcf::dcf_add_bundle("bundle_injury_overdose")


##Process bundle
#run these from the relevant bundle directory
# dcf::dcf_process("bundle_respiratory", ".")
# dcf::dcf_process("bundle_childhood_immunizations", ".")
# dcf::dcf_process("chronic_diseases", ".")
# dcf::dcf_process("bundle_injury_overdose", ".")

#Update mermaid diagram
#dcf_status_diagram()


#dcf::dcf_status_diagram()

#dcf::dcf_init() sets up github actions