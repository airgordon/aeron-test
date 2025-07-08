package org.example;

import io.aeron.Aeron;
import io.aeron.FragmentAssembler;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.FragmentHandler;
import org.agrona.BufferUtil;
import org.agrona.CloseHelper;
import org.agrona.concurrent.BusySpinIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.SigInt;
import org.agrona.concurrent.UnsafeBuffer;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class Main {
    private static final int PING_STREAM_ID = 23;
    private static final int PONG_STREAM_ID = 24;
    private static final int FRAGMENT_COUNT_LIMIT = 17;
    private static final String PING_CHANNEL = "aeron:udp?endpoint=0.0.0.0:20123";
    public static final int SEND_TRIES = 100_000;
    private static String PONG_CHANNEL = "aeron:udp?endpoint=host.docker.internal:20124";

    private static final boolean EMBEDDED_MEDIA_DRIVER = true;

    private static long message = 0;

    public static void main(String[] args) {

        System.out.println("Subscribing to " + PING_CHANNEL);
        final MediaDriver.Context mctx = new MediaDriver.Context();
        mctx.threadingMode(ThreadingMode.SHARED);
        final MediaDriver driver = EMBEDDED_MEDIA_DRIVER ? MediaDriver.launchEmbedded(mctx) : null;
        final Aeron.Context ctx = new Aeron.Context()
                .availableImageHandler(image -> {
                    Subscription subscription = image.subscription();
                    System.out.println(PONG_CHANNEL);
                    System.out.printf(
                            "Available image on %s streamId=%d sessionId=%d mtu=%d term-length=%d from %s%n",
                            subscription.channel(), subscription.streamId(), image.sessionId(), image.mtuLength(),
                            image.termBufferLength(), image.sourceIdentity());
                })
                .unavailableImageHandler(SamplesUtil::printAvailableImage);

        if (EMBEDDED_MEDIA_DRIVER) {
            ctx.aeronDirectoryName(driver.aeronDirectoryName());
        }

        final FragmentHandler fragmentHandler = (buffer, offset, length, header) ->
        {
            final long msg = buffer.getLong(offset);
            message = msg;
//            System.out.println(System.nanoTime() - Long.parseLong(msg.split(" ")[2]));
//            System.out.printf(
//                    "Message to stream %d from session %d (%d@%d) <<%s>>%n",
//                    STREAM_ID, header.sessionId(), length, offset, msg);
        };
        final AtomicBoolean running = new AtomicBoolean(true);

        // Register a SIGINT handler for graceful shutdown.
        SigInt.register(() -> running.set(false));

        boolean firstMsg = true;

        // Create an Aeron instance using the configured Context and create a
        // Subscription on that instance that subscribes to the configured
        // channel and stream ID.
        // The Aeron and Subscription classes implement "AutoCloseable" and will automatically
        // clean up resources when this try block is finished
        try (Aeron aeron = Aeron.connect(ctx);
             Subscription subscription = aeron.addSubscription(PING_CHANNEL, PING_STREAM_ID);
             Publication publication = aeron.addPublication(PONG_CHANNEL, PONG_STREAM_ID)
        ) {
            final UnsafeBuffer buffer = new UnsafeBuffer(BufferUtil.allocateDirectAligned(256, 64));
            final FragmentAssembler assembler = new FragmentAssembler(fragmentHandler);
            while (running.get()) {
                while (true) {
                    final int fragmentsRead = subscription.poll(assembler, FRAGMENT_COUNT_LIMIT);
                    if (fragmentsRead > 0) {
                        if (firstMsg) {
                            System.out.println("got message");
                        }
                        break;
                    }
                }
//                while (true) {
//                    if (publication.isConnected()) {
//                        System.out.println("PONG connected sessionId=" + publication.sessionId());
//                        break;
//                    }
//                }
                for (int sendTry = 0; sendTry < SEND_TRIES; sendTry++) {

                    buffer.putLong(0, message);
                    long position = publication.offer(buffer, 0, 8);
                    if (position > 0) {
                        if (firstMsg) {
                            System.out.println(sendTry + " yay!");
                            firstMsg = false;
                        }
                        break;
                    }
//                    else if (position == Publication.BACK_PRESSURED) {
//                        System.out.println("Offer failed due to back pressure");
//                    } else if (position == Publication.NOT_CONNECTED) {
//                        System.out.println("Offer failed because publisher is not connected to a subscriber");
//                    } else if (position == Publication.ADMIN_ACTION) {
//                        System.out.println("Offer failed because of an administration action in the system");
//                    } else if (position == Publication.CLOSED) {
//                        System.out.println("Offer failed because publication is closed");
//                        break;
//                    } else if (position == Publication.MAX_POSITION_EXCEEDED) {
//                        System.out.println("Offer failed due to publication reaching its max position");
//                        break;
//                    } else {
//                        System.out.println("Offer failed due to unknown reason: " + position);
//                    }
                }
            }

            System.out.println("Shutting down...");
        }

        CloseHelper.close(driver);
    }
}