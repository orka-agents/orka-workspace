FROM scratch
COPY --chmod=0555 native-data-proof /native-data-proof
ENTRYPOINT ["/native-data-proof"]
