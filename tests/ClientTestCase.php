<?php

namespace Keboola\Test;

use Keboola\StorageApi\Client;
use PHPUnit\Framework\TestCase;

class ClientTestCase extends TestCase
{
    public function getClient(array $options)
    {
        $options['userAgent'] = sprintf('SAPI test: %s\\%s', get_class($this), $this->getName());
        return new Client($options);
    }
}
