<?php

namespace Keboola\Test\ComponentsUtils;

use Keboola\StorageApi\Options\Components\Configuration;

trait ComponentsConfigurationUtils
{
    public function createConfiguration($components, $componentId, $configurationId, $name)
    {
        $configurationOptions = (new Configuration())
            ->setComponentId($componentId)
            ->setConfigurationId($configurationId)
            ->setName($name);

        $components->addConfiguration($configurationOptions);
        return $configurationOptions;
    }
}
