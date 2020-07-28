<?php

namespace Keboola\StorageApi;

use Symfony\Component\Process\Process;

final class ProcessPolyfill
{
    /**
     * @param string $cmdString
     * @return Process
     */
    public static function createProcess($cmdString)
    {
        if (method_exists(Process::class, 'fromShellCommandline')) {
            return Process::fromShellCommandline($cmdString);
        }

        return new Process($cmdString);
    }
}
