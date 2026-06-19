var cmd_1 = new java.lang.ProcessBuilder()
    .command("/bin/sh", "-c", "ssh ${CLIENT} 'nohup java -jar /tmp/Bucket-sizer.jar --bucket=${BUCKET} --pages=5 --no-versions --quiet > /tmp/listing-results.out &'")
    .inheritIO()
    .start();
cmd_1.waitFor();
