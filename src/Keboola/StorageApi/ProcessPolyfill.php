<?php

namespace Keboola\StorageApi;

use ReflectionClass;
use Symfony\Component\Process\Process;

final class ProcessPolyfill
{
    /**
     * @param string $cmdString
     * @return Process
     */
    public static function createProcess($cmdString)
    {
        $ref = new ReflectionClass(Process::class);
        if ($ref->hasMethod('fromShellCommandline')) {
            return $ref->getMethod('fromShellCommandline')->invoke(null, $cmdString);
        }

        return $ref->newInstance($cmdString);
    }
}
