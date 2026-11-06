<?php

declare(strict_types=1);

namespace Keboola\UnitTest\Downloader;

use RuntimeException;

/** One running instance of the stalling-blob-server.php fixture. */
final class StallingBlobServerProcess
{
    /** @var resource */
    private $process;

    /** @var array<int, resource> */
    private array $pipes;

    public readonly int $port;

    /**
     * @param resource $process
     * @param array<int, resource> $pipes
     */
    private function __construct($process, array $pipes, int $port)
    {
        $this->process = $process;
        $this->pipes = $pipes;
        $this->port = $port;
    }

    public static function start(int $stallSeconds, int $bodyBytes, ?string $attemptLog): self
    {
        $arguments = [
            PHP_BINARY,
            __DIR__ . '/stalling-blob-server.php',
            (string) $stallSeconds,
            (string) $bodyBytes,
        ];
        if ($attemptLog !== null) {
            $arguments[] = $attemptLog;
        }

        $process = proc_open(
            $arguments,
            [1 => ['pipe', 'w'], 2 => ['pipe', 'w']],
            $pipes,
        );
        if ($process === false) {
            throw new RuntimeException('cannot start the stalling blob server');
        }

        $announcement = fgets($pipes[1]);
        if (!is_string($announcement) || !str_starts_with($announcement, 'PORT=')) {
            (new self($process, $pipes, 0))->stop();
            throw new RuntimeException('the stalling server did not report its port');
        }

        return new self($process, $pipes, (int) substr(trim($announcement), strlen('PORT=')));
    }

    public function stop(): void
    {
        foreach ($this->pipes as $pipe) {
            if (is_resource($pipe)) {
                fclose($pipe);
            }
        }
        $this->pipes = [];
        if (is_resource($this->process)) {
            proc_terminate($this->process);
            proc_close($this->process);
        }
    }
}
