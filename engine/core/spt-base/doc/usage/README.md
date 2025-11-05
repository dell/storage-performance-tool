# Contents

### 1. [Input](input)
&nbsp;&nbsp;1.1. [Configuration](input/configuration)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;1.1.1. [Configuration Reference Table](input/configuration#11-reference-table)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;1.1.2. [Expressions](input/configuration#124-expression)<br/>
&nbsp;&nbsp;1.2. [CLI](input/cli)<br/>
&nbsp;&nbsp;1.3. [Scenarios](input/scenarios)<br/>
    
### 2. [Output](output)
**&nbsp;&nbsp;2.1. [General Output](output#1-general)<br/>**
&nbsp;&nbsp;&nbsp;&nbsp;2.1.1. [Logging](output#11-logging-subsystem)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;2.1.1.1. [Logs Separation By Load Step Id](output#111-load-step-id)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;2.1.1.2. [Console](output#112-console)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;2.1.1.3. [Files](output#113-files)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;2.1.1.4. [Configuration](output#114-log-configuration)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;2.1.2. [Output Categories](output#12-categories)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;2.1.2.1. [CLI arguments log](output#121-cli-arguments)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;2.1.2.2. [Configuration dump](output#122-configuration-dump)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;2.1.2.3. [Scenario dump](output#123-scenario-dump)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;2.1.2.4. [3rd Party Messages](output#124-3rd-party-log-messages)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;2.1.2.5. [Error Messages](output#125-error-messages)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;2.1.2.6. [General Messages](output#126-general-messages)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;2.1.2.7. [Item List Files](output#127-item-list-files)<br/>

**&nbsp;&nbsp;2.2. [Metrics Output](output#2-metrics)<br/>**
&nbsp;&nbsp;&nbsp;&nbsp;2.2.1. [Load Average](output#21-load-average)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;2.2.2. [Load Step Summary](output#22-load-step-summary)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;2.2.3. [Operation Traces](output#23-operation-traces)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;2.2.4. [Accounting Activation By The Threshold](output#24-threshold)<br/>

### 3. Load Generation
**&nbsp;&nbsp;3.1. [Items](item)<br/>**
&nbsp;&nbsp;&nbsp;&nbsp;3.1.1. [Item Types](item/types)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;3.1.1.1. [Data Item](item/types#1-data)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;3.1.1.1.1. [Data Item Size](item/types#11-size)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;3.1.1.1.1.1. [Fixed Size](item/types#111-fixed)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;3.1.1.1.1.2. [Random Size](item/types#112-random)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;3.1.1.1.2. [Data Item Payload](item/types#12-payload)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;3.1.1.1.2.1. [Random Payload Using A Seed](item/types#121-random-using-a-seed)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;3.1.1.1.2.2. [Custom Payload Using An External File](item/types#122-custom-using-an-external-file)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;3.1.1.2. [Path Item](item/types#2-path)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;3.1.1.3. [Token Item](item/types#3-token)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;3.1.2. [Item Input](item/input)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;3.1.2.1. [Item Input File](item/input#1-file)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;3.1.2.2. [Item Path Listing Input](item/input#2-item-path-listing-input)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;3.1.2.3. [New Items Input](item/input#3-new-items-input)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;3.1.2.3.1. [Item Naming](item/input#31-naming)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;3.1.2.3.1.1. [Types](item/input#311-types)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;3.1.2.3.1.1.1. [Random](item/input#3111-random)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;3.1.2.3.1.1.2. [Serial](item/input#3112-serial)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;3.1.2.3.1.2. [Prefix](item/input#312-prefix)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;3.1.2.3.1.3. [Radix](item/input#313-radix)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;3.1.2.3.1.4. [Seed](item/input#314-seed)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;3.1.2.3.1.5. [Length](item/input#315-length)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;3.1.3. [Item Output](item/output)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;3.1.3.1. [Item Output File](item/output#1-file)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;3.1.3.2. [Item Output Path](item/output#2-path)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;3.1.3.2.1. [Variable Path](item/output#21-variable)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;3.1.3.2.1.1. [Multiuser Variable Path](item/output#211-multiuser)<br/>

**&nbsp;&nbsp;3.2. Load Operations<br/>**
&nbsp;&nbsp;&nbsp;&nbsp;3.2.1. [Operation Types](load/operations/types)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;3.2.1.1. [Create](load/operations/types#1-create)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;3.2.1.1.1. [Basic](load/operations/types#11-basic)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;3.2.1.1.2. [Copy Mode](load/operations/types#12-copy-mode)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;3.2.1.2. [Read](load/operations/types#2-read)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;3.2.1.2.1. [Basic](load/operations/types#21-basic)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;3.2.1.2.2. [Content Verification](load/operations/types#22-content-verification)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;3.2.1.3. [Update](load/operations/types#3-update)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;3.2.1.4. [Delete](load/operations/types#4-delete)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;3.2.1.5. [Noop](load/operations/types#5-noop)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;3.2.2. [Byte Ranges Operations](load/operations/byte_ranges)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;3.2.2.1. [Random Ranges](load/operations/byte_ranges#41-random-ranges)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;3.2.2.2. [Fixed Ranges](load/operations/byte_ranges#42-fixed-ranges)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;3.2.2.2.1. [Append](load/operations/byte_ranges#421-append)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;3.2.3. [Composite Operations](load/operations/composite)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;3.2.3.1. [Storage-Side Concatenation](load/operations/composite#1-storage-side-concatenation)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;3.2.3.1.1. [S3 Multipart Upload](load/operations/composite#131-s3-multipart-upload)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;3.2.3.1.2. [Swift Dynamic Large Objects](load/operations/composite#132-swift-dynamic-large-objects)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;3.2.4. [Operations Recycling](load/operations/recycling)<br/>

**&nbsp;&nbsp;3.3. [Load Steps](load/steps)<br/>**
&nbsp;&nbsp;&nbsp;&nbsp;3.3.1. [Step Identification](load/steps#1-identification)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;3.3.2. [Step Types](load/steps#3-type) <br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;3.3.2.1. [Linear Load](load/steps#31-linear)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;3.3.2.2. [Pipeline Load](https://github.com/dell/storage-performance-tool/tree/main/engine/extensions/load-steps/pipeline)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;3.3.2.3. [Weighted Load](https://github.com/dell/storage-performance-tool/tree/main/engine/extensions/load-steps/weighted)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;3.3.3. [Step Limits](load/steps#2-limits)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;3.3.3.1. [Operations Count](load/steps#21-operations-count)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;3.3.3.2. [Time](load/steps#22-time)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;3.3.3.3. [Transfer Size](load/steps#23-transfer-size)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;3.3.3.4. [End Of Input](load/steps#24-end-of-input)<br/>


### 4. [Load Scaling](scaling)
&nbsp;&nbsp;4.1. [Rate](scaling#1-rate)<br/>
&nbsp;&nbsp;4.2. [Concurrency](scaling#2-concurrency)<br/>
&nbsp;&nbsp;4.3. [Distributed Mode](scaling#3-distributed-mode)<br/>
