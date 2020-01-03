/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.prestosql.tests.product.launcher.env.environment;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import io.prestosql.tests.product.launcher.cli.PathResolver;
import io.prestosql.tests.product.launcher.docker.DockerFiles;
import io.prestosql.tests.product.launcher.env.Environment;
import io.prestosql.tests.product.launcher.env.EnvironmentOptions;
import io.prestosql.tests.product.launcher.env.EnvironmentProvider;
import org.rnorth.ducttape.TimeoutException;
import org.rnorth.ducttape.unreliables.Unreliables;
import org.testcontainers.containers.ContainerLaunchException;
import org.testcontainers.containers.FixedHostPortGenericContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.startupcheck.IsRunningStartupCheckStrategy;
import org.testcontainers.containers.wait.internal.ExternalPortListeningCheck;
import org.testcontainers.containers.wait.internal.InternalCommandPortListeningCheck;
import org.testcontainers.containers.wait.strategy.AbstractWaitStrategy;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.containers.wait.strategy.WaitAllStrategy;

import javax.inject.Inject;

import java.io.File;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static org.testcontainers.containers.BindMode.READ_ONLY;

public class Standard
        implements EnvironmentProvider
{
    private static final Logger log = Logger.get(Standard.class);

    private final PathResolver pathResolver;
    private final DockerFiles dockerFiles;

    private final String hadoopBaseImage;
    private final String imagesVersion;
    private final File serverPackage;

    // TODO consider move this to test-run or get from ENV
    private final File jdbcJar;

    // TODO consider move this to test-run or get from ENV
    private final File cliJar;

    @Inject
    public Standard(
            PathResolver pathResolver,
            DockerFiles dockerFiles,
            EnvironmentOptions environmentOptions)
    {
        this.pathResolver = requireNonNull(pathResolver, "pathResolver is null");
        this.dockerFiles = requireNonNull(dockerFiles, "dockerFiles is null");
        requireNonNull(environmentOptions, "environmentOptions is null");
        hadoopBaseImage = requireNonNull(environmentOptions.hadoopBaseImage, "environmentOptions.hadoopBaseImage is null");
        imagesVersion = requireNonNull(environmentOptions.imagesVersion, "environmentOptions.imagesVersion is null");
        serverPackage = requireNonNull(environmentOptions.serverPackage, "environmentOptions.serverPackage is null");
        checkArgument(serverPackage.getName().endsWith(".tar.gz"), "Currently only server .tar.gz package is supported");
        jdbcJar = requireNonNull(environmentOptions.jdbcJar, "environmentOptions.jdbcJar is null");
        cliJar = requireNonNull(environmentOptions.cliJar, "environmentOptions.cliJar is null");
    }

    @Override
    @SuppressWarnings("resource")
    public Environment.Builder createEnvironment()
    {
        Environment.Builder builder = Environment.builder();

        builder.addContainer("hadoop-master", createHadoopMaster());
        builder.addContainer("presto-master", createPrestoMaster());
        builder.addContainer("tests", createTestsContainer());

        return builder;
    }

    @SuppressWarnings("resource")
    private GenericContainer<?> createHadoopMaster()
    {
        FixedHostPortGenericContainer<?> container = new FixedHostPortGenericContainer<>(hadoopBaseImage + ":" + imagesVersion)
                // TODO HIVE_PROXY_PORT:1180
                .withFileSystemBind(dockerFiles.getDockerFilesHostPath().toString(), "/docker/files", READ_ONLY)
                .withStartupCheckStrategy(new IsRunningStartupCheckStrategy())
                .waitingFor(new HostPortWaitStrategy(10000)) // HiveServer2
                .withStartupTimeout(Duration.ofMinutes(5));

        exposePort(container, 5006); // debug port
        exposePort(container, 8020);
        exposePort(container, 8042);
        exposePort(container, 8088);
        exposePort(container, 9000);
        exposePort(container, 9083); // Metastore Thrift
        exposePort(container, 9864); // DataNode Web UI since Hadoop 3
        exposePort(container, 9870); // NameNode Web UI since Hadoop 3
        exposePort(container, 10000); // HiveServer2
        exposePort(container, 19888);
        exposePort(container, 50070); // NameNode Web UI prior to Hadoop 3
        exposePort(container, 50075); // DataNode Web UI prior to Hadoop 3

        return container;
    }

    @SuppressWarnings("resource")
    private GenericContainer<?> createPrestoMaster()
    {
        FixedHostPortGenericContainer<?> container = new FixedHostPortGenericContainer<>("prestodev/centos7-oj11:" + imagesVersion)
                .withFileSystemBind(dockerFiles.getDockerFilesHostPath().toString(), "/docker/files", READ_ONLY)
                .withFileSystemBind(pathResolver.resolvePlaceholders(serverPackage).toString(), "/docker/presto-server.tar.gz", READ_ONLY)
                .withCommand("/docker/files/presto-launcher-wrapper.sh", "singlenode", "run")
                .withStartupCheckStrategy(new IsRunningStartupCheckStrategy())
                .waitingFor(Wait.forLogMessage(".*======== SERVER STARTED ========.*", 1))
                .withStartupTimeout(Duration.ofMinutes(5));

        exposePort(container, 8080); // Presto default port
        exposePort(container, 5005); // debug port

        return container;
    }

    @SuppressWarnings("resource")
    private GenericContainer<?> createTestsContainer()
    {
        FixedHostPortGenericContainer<?> container = new FixedHostPortGenericContainer<>("prestodev/centos6-oj8:" + imagesVersion)
                .withFileSystemBind(dockerFiles.getDockerFilesHostPath().toString(), "/docker/files", READ_ONLY)
                .withCommand("bash", "-xeuc", "echo 'No command provided' >&2; exit 69")
                .waitingFor(new WaitAllStrategy()) // don't wait
                .withStartupCheckStrategy(new IsRunningStartupCheckStrategy());

        exposePort(container, 5007); // debug port

        return container;
    }

    private static void exposePort(FixedHostPortGenericContainer<?> container, int port)
    {
        container.addExposedPort(port);
        container.withFixedExposedPort(port, port);
    }

    /**
     * TODO rename and make concise as https://github.com/testcontainers/testcontainers-java/pull/2224 was dropped
     */
    public static class HostPortWaitStrategy
            extends AbstractWaitStrategy
    {
        private final List<Integer> ports;

        public HostPortWaitStrategy(int port)
        {
            this(ImmutableList.of(port));
        }

        public HostPortWaitStrategy(List<Integer> ports)
        {
            this.ports = ImmutableList.copyOf(requireNonNull(ports, "ports is null"));
        }

        @Override
        protected void waitUntilReady()
        {
            final Set<Integer> externalLivenessCheckPorts = getLivenessCheckPorts();
            if (externalLivenessCheckPorts.isEmpty()) {
                if (log.isDebugEnabled()) {
                    log.debug("Liveness check ports of {} is empty. Not waiting.", waitStrategyTarget.getContainerInfo().getName());
                }
                return;
            }

            List<Integer> exposedPorts = ports;

            final Set<Integer> internalPorts = getInternalPorts(externalLivenessCheckPorts, exposedPorts);

            Callable<Boolean> internalCheck = new InternalCommandPortListeningCheck(waitStrategyTarget, internalPorts);

            Callable<Boolean> externalCheck = new ExternalPortListeningCheck(waitStrategyTarget, ImmutableSet.copyOf(exposedPorts));

            try {
                Unreliables.retryUntilTrue((int) startupTimeout.getSeconds(), TimeUnit.SECONDS,
                        () -> getRateLimiter().getWhenReady(() -> internalCheck.call() && externalCheck.call()));
            }
            catch (TimeoutException e) {
                throw new ContainerLaunchException("Timed out waiting for container port to open (" +
                        waitStrategyTarget.getContainerIpAddress() +
                        " ports: " +
                        exposedPorts +
                        " should be listening)");
            }
        }

        private Set<Integer> getInternalPorts(Set<Integer> externalLivenessCheckPorts, List<Integer> exposedPorts)
        {
            return exposedPorts.stream()
                    .filter(it -> externalLivenessCheckPorts.contains(waitStrategyTarget.getMappedPort(it)))
                    .collect(Collectors.toSet());
        }
    }
}
