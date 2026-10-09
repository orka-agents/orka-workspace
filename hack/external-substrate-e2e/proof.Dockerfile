FROM scratch
COPY native-data-proof /native-data-proof
USER 65532:65532
ENTRYPOINT ["/native-data-proof"]
