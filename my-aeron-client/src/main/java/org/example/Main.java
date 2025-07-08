package org.example;

import io.aeron.*;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.FragmentHandler;
import org.agrona.BufferUtil;
import org.agrona.CloseHelper;
import org.agrona.concurrent.UnsafeBuffer;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

public class Main {
    private static final int PING_STREAM_ID = 23;
    private static final int PONG_STREAM_ID = 24;
    private static final String PING_CHANNEL = "aeron:udp?endpoint=localhost:20123";
    private static final String PONG_CHANNEL = "aeron:udp?endpoint=0.0.0.0:20124";

    private static final int NUMBER_OF_MESSAGES = 100_000;
    private static final int BATCH_SIZE = 1000;
    private static final int TRIES = 10_000_000;
    private static final int FRAGMENT_COUNT_LIMIT = 10;
    private static final long LINGER_TIMEOUT_MS = 1000;
    private static final boolean EMBEDDED_MEDIA_DRIVER = true;

    /**
     * Main method for launching the process.
     * <p>
     * Run configuration > Modify options > Add VM options
     * VM options:
     * --add-opens java.base/jdk.internal.misc=ALL-UNNAMED --add-exports java.base/jdk.internal.misc=ALL-UNNAMED
     */
    public static void main(final String[] args) throws InterruptedException {

//        UdpServer.runServer(20125);

        System.out.println("Publishing to " + PING_CHANNEL + " on stream id " + PING_STREAM_ID);

        long[] times = new long[NUMBER_OF_MESSAGES];
        int[] sendTriesArr = new int[NUMBER_OF_MESSAGES];
        int[] respTriesArr = new int[NUMBER_OF_MESSAGES];
        AtomicInteger msgIdx = new AtomicInteger();

        // If configured to do so, create an embedded media driver within this application rather
        // than relying on an external one.
        final MediaDriver.Context mctx = new MediaDriver.Context();
        mctx.threadingMode(ThreadingMode.SHARED);
        final MediaDriver driver = EMBEDDED_MEDIA_DRIVER ? MediaDriver.launchEmbedded(mctx) : null;

        final Aeron.Context ctx = new Aeron.Context();
        ctx.errorHandler(System.out::println)
                .availableImageHandler(Main::printAvailableImage)
                .unavailableImageHandler(Main::printUnavailableImage);

        if (EMBEDDED_MEDIA_DRIVER) {
            ctx.aeronDirectoryName(driver.aeronDirectoryName());
        }

        final UnsafeBuffer sendBuffer = new UnsafeBuffer(BufferUtil.allocateDirectAligned(256, 64));

        final FragmentHandler fragmentHandler = (buffer, offset, length, header) ->
        {
//            System.out.println("got message " + buffer.getLong(offset));
            times[msgIdx.getAndIncrement()] = System.nanoTime() - buffer.getLong(offset);

            //            System.out.println(System.nanoTime() - Long.parseLong(msg.split(" ")[2]));
//            System.out.printf(
//                    "Message to stream %d from session %d (%d@%d) <<%d>>%n",
//                    STREAM_ID, header.sessionId(), length, offset, buffer.getLong(offset));
        };
        final FragmentAssembler assembler = new FragmentAssembler(fragmentHandler);


        // Connect a new Aeron instance to the media driver and create a publication on
        // the given channel and stream ID.
        // The Aeron and Publication classes implement "AutoCloseable" and will automatically
        // clean up resources when this try block is finished
        try (Aeron aeron = Aeron.connect(ctx);
             Publication publication = aeron.addPublication(PING_CHANNEL, PING_STREAM_ID);
             Subscription subscription = aeron.addSubscription(PONG_CHANNEL, PONG_STREAM_ID)) {

            System.out.println("waiting subscr conn");
            while (true) {
                if (subscription.isConnected())
                    break;
            }
            System.out.println("PONG connected");
            subscription.images().forEach(image -> System.out.println("- PONG subscr sessionId=" + image.sessionId()));


            while (true) {
                if (publication.isConnected()) {
                    break;
                }
            }
            System.out.println("PING connected sessionId=" + publication.sessionId());

            for (long batch = 0; batch < NUMBER_OF_MESSAGES / BATCH_SIZE; batch++) {
                for (long msgInBatch = 0; msgInBatch < BATCH_SIZE; msgInBatch++) {
//                System.out.print("Offering " + i + "/" + NUMBER_OF_MESSAGES + " - ");

                    long start = System.nanoTime();
                    int sendTries = 0;
                    long position = -1000;
                    for (; sendTries < TRIES; sendTries++) {
//                        System.out.println("sending message " + start);
                        sendBuffer.putLong(0, start);
                        position = publication.offer(sendBuffer, 0, 8);
//                System.out.println("took" + (System.nanoTime() - start));

                        if (position > 0) {
//                            System.out.println(sendTries + " yay!");
                            break;
                        } else if (position == Publication.BACK_PRESSURED) {
                            System.out.println("Offer failed due to back pressure");
                        } else if (position == Publication.NOT_CONNECTED) {
                            System.out.println("Offer failed because publisher is not connected to a subscriber");
                        } else if (position == Publication.ADMIN_ACTION) {
                            System.out.println("Offer failed because of an administration action in the system");
                        } else if (position == Publication.CLOSED) {
                            System.out.println("Offer failed because publication is closed");
                            throw new RuntimeException("Offer failed because publication is closed");
                        } else if (position == Publication.MAX_POSITION_EXCEEDED) {
                            System.out.println("Offer failed due to publication reaching its max position");
                            throw new RuntimeException("Offer failed due to publication reaching its max position");
                        } else {
                            System.out.println("Offer failed due to unknown reason: " + position);
                        }
                    }
                    if (position <= 0) {
                        throw new IllegalStateException("Did bot send " + position);
                    }

                    sendTriesArr[msgIdx.get()] = sendTries;

//                    if (!publication.isConnected()) {
//                        System.out.println("No active subscribers detected");
//                    }
                    long sentId = batch * BATCH_SIZE + msgInBatch;

//                    System.out.println("waaaaaaaiting resp");
                    int respTries = 0;
                    for (; respTries < TRIES; respTries++) {
//                        System.out.println("poll " + respTries);
                        int fragments = subscription.poll(assembler, FRAGMENT_COUNT_LIMIT);
//                        if (fragments != 0){
//                            System.out.println(fragments);
                        if (sentId != msgIdx.get()) {
//                            System.out.println("got fragment");
                            break;
                        }
                    }
                    if (sentId == msgIdx.get()) {
                        throw new RuntimeException("Did not get resp");
                    }
                    respTriesArr[msgIdx.get() - 1] = respTries;
                }
                System.out.println("sent batch " + batch);
            }
        }

        System.out.println("Done sending.");

        {
            System.out.println();
            System.out.println("sendTries");
            Arrays.sort(sendTriesArr);
            System.out.println("25 " + sendTriesArr[NUMBER_OF_MESSAGES / 4]);
            System.out.println("50 " + sendTriesArr[NUMBER_OF_MESSAGES * 2 / 4]);
            System.out.println("75 " + sendTriesArr[NUMBER_OF_MESSAGES * 3 / 4]);
            System.out.println("90 " + sendTriesArr[NUMBER_OF_MESSAGES * 9 / 10]);
            System.out.println("99 " + sendTriesArr[NUMBER_OF_MESSAGES * 99 / 100]);
            System.out.println(IntStream.of(sendTriesArr).summaryStatistics());
        }

        {
            System.out.println();
            System.out.println("respTries");
            Arrays.sort(respTriesArr);
            System.out.println("25 " + respTriesArr[NUMBER_OF_MESSAGES / 4]);
            System.out.println("50 " + respTriesArr[NUMBER_OF_MESSAGES * 2 / 4]);
            System.out.println("75 " + respTriesArr[NUMBER_OF_MESSAGES * 3 / 4]);
            System.out.println("90 " + respTriesArr[NUMBER_OF_MESSAGES * 9 / 10]);
            System.out.println("99 " + respTriesArr[NUMBER_OF_MESSAGES * 99 / 100]);
            System.out.println(IntStream.of(respTriesArr).summaryStatistics());
        }

        {
            System.out.println();
            System.out.println("times");
            Arrays.sort(times);
            System.out.println("25 " + times[NUMBER_OF_MESSAGES / 4]);
            System.out.println("50 " + times[NUMBER_OF_MESSAGES * 2 / 4]);
            System.out.println("75 " + times[NUMBER_OF_MESSAGES * 3 / 4]);
            System.out.println("90 " + times[NUMBER_OF_MESSAGES * 9 / 10]);
            System.out.println("99 " + times[NUMBER_OF_MESSAGES * 99 / 100]);
            System.out.println(LongStream.of(times).summaryStatistics());
        }

        if (LINGER_TIMEOUT_MS > 0) {
            System.out.println("Lingering for " + LINGER_TIMEOUT_MS + " milliseconds...");
            Thread.sleep(LINGER_TIMEOUT_MS);
        }
        CloseHelper.close(driver);
    }


    /**
     * Print the information for an available image to stdout.
     *
     * @param image that has been created.
     */
    public static void printAvailableImage(final Image image) {
        final Subscription subscription = image.subscription();
        System.out.printf(
                "Available image on %s streamId=%d sessionId=%d mtu=%d term-length=%d from %s%n",
                subscription.channel(), subscription.streamId(), image.sessionId(), image.mtuLength(),
                image.termBufferLength(), image.sourceIdentity());
    }

    /**
     * Print the information for an unavailable image to stdout.
     *
     * @param image that has gone inactive.
     */
    public static void printUnavailableImage(final Image image) {
        final Subscription subscription = image.subscription();
        System.out.printf(
                "Unavailable image on %s streamId=%d sessionId=%d%n",
                subscription.channel(), subscription.streamId(), image.sessionId());
    }

}

