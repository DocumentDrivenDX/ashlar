# Commerce export and Fabric availability

The existing-publication configuration landed on main as `26bac923bbaa436df60a489e0f9e48a6ac9be80f`. Five focused CLI tests passed in 0.757 seconds; all five automatic CI workflows passed. Independent review accepted the configuration change.

One actual read-only export selected the retained original commerce publication, existing ordinary PostgreSQL source/ACK configuration, Spark local[1] with 512 MiB, and the existing Delta JARs. It used current repository source and retained SDK selections; this was not a new installed-package qualification. The selected publication was `/private/tmp/ashlar-original-commerce-native-20261011-e/publication`.

The export did not complete. Repeated source checks continued during closing after the 120-second alarm. The operator terminated the owned Spark child; the parent exited with status 1. Both process IDs were subsequently absent, and the selected export output directory did not exist. No export pair, successful custody receipt, or Fabric deployment is claimed. Do not repeat this run until the repeated checks are addressed.

Opening the existing Ashlar_Graph_Releases lakehouse in Fabric returned “Capacity limit exceeded” and stated that the organization's Fabric compute capacity exceeded its limits. No capacity, licensing, access, or other settings were changed. This blocks the current cloud load attempt only.
