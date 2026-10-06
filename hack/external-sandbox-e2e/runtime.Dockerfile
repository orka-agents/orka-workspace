FROM busybox:1.37.0
USER 65532:65532
ENTRYPOINT ["/bin/sh", "-c", "exec httpd -f -p 8080 -h /durable/orka-workspace"]
