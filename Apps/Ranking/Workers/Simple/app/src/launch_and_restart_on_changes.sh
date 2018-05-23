#!/usr/bin/env bash
daemon() {
    chsum1=""
    jobId=0

    while [[ true ]]
    do
        chsum2=`find algorithms/ -type f -exec md5sum {} \;`
        if [[ $chsum1 != $chsum2 ]] ; then
            echo "Changes detected!"
            if [[ $jobId != 0 ]] ; then
              echo "Killing previous job..."
              kill $jobId
            fi
              ./start.py &
            jobId=$!
            chsum1=$chsum2
        fi
        sleep 2
    done
}

trap 'kill $(jobs -p)' EXIT
daemon

