<?php

declare(strict_types=1);

namespace Keboola\Test\Utils;

use DateTimeInterface;
use Flow\Parquet\Reader;

trait ParquetUtils
{
    /**
     * @param string[] $files
     * @return array<int<0, max>, array<string, mixed>>
     */
    public function getParquetContent(array $files): array
    {
        $content = [];
        $reader = new Reader();
        foreach ($files as $tmpFile) {
            $file = $reader->read($tmpFile);
            foreach ($file->values() as $row) {
                foreach ($row as $column => &$value) {
                    if ($value instanceof DateTimeInterface) {
                        $row[$column] = $value->format(DateTimeInterface::ATOM);
                    }
                }
                $content[] = $row;
            }
        }
        return $content;
    }
}
