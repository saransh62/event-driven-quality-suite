package com.eventsuite.e2e;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/** Stops/starts compose services. Uses `docker compose` (v2 plugin) or falls back to `docker-compose`. */
final class DockerCompose {

    private static final List<String> BASE = detect();

    private DockerCompose() {
    }

    static void stop(String service) {
        run("stop", service);
    }

    static void start(String service) {
        run("start", service);
    }

    private static void run(String... args) {
        List<String> cmd = new ArrayList<>(BASE);
        cmd.addAll(List.of("-f", Env.COMPOSE_FILE));
        cmd.addAll(List.of(args));
        if (exec(cmd) != 0) {
            throw new IllegalStateException("Command failed: " + String.join(" ", cmd));
        }
    }

    private static List<String> detect() {
        return exec(List.of("docker", "compose", "version")) == 0
                ? List.of("docker", "compose")
                : List.of("docker-compose");
    }

    private static int exec(List<String> cmd) {
        try {
            Process p = new ProcessBuilder(cmd).inheritIO().start();
            if (!p.waitFor(120, TimeUnit.SECONDS)) {
                p.destroyForcibly();
                return -1;
            }
            return p.exitValue();
        } catch (IOException e) {
            return -1;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return -1;
        }
    }
}
