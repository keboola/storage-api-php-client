<?php

declare(strict_types=1);

namespace Keboola\StorageApi\Exporter;

enum FileType: string
{
    case CSV = 'csv';
    case PARQUET = 'parquet';
}
