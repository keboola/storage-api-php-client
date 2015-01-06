<?php
/**
 *
 * User: Martin Halamíček
 * Date: 16.5.12
 * Time: 11:46
 *
 */


use Keboola\StorageApi\Client;

use Keboola\Csv\CsvFile;

class Keboola_StorageApi_Tables_SlicedImportsTest extends StorageApiTestCase
{

	public function setUp()
	{
		parent::setUp();
		$this->_initEmptyBucketsForAllBackends();
	}

	/**
	 * @dataProvider backends
	 * @param $backend
	 */
	public function testSlicedImportGzipped($backend)
	{

		$uploadOptions = new \Keboola\StorageApi\Options\FileUploadOptions();
		$uploadOptions
			->setFileName('entries_')
			->setIsEncrypted(false)
			->setIsSliced(true);
		$slicedFile = $this->_client->prepareFileUpload($uploadOptions);

		$uploadParams = $slicedFile['uploadParams'];
		$credentials = new Aws\Common\Credentials\Credentials(
			$uploadParams['credentials']['AccessKeyId'],
			$uploadParams['credentials']['SecretAccessKey'],
			$uploadParams['credentials']['SessionToken']
		);

		$s3Client = \Aws\S3\S3Client::factory(array('credentials' => $credentials));
		$s3Client->putObject(array(
			'Bucket' => $uploadParams['bucket'],
			'Key'    => $uploadParams['key'] . 'part001.gz',
			'Body'   => fopen(__DIR__ . '/../_data/sliced/neco_0000_part_00.gz', 'r+'),
		))->get('ObjectURL');

		$s3Client->putObject(array(
			'Bucket' => $uploadParams['bucket'],
			'Key'    => $uploadParams['key'] . 'part002.gz',
			'Body'   => fopen(__DIR__ . '/../_data/sliced/neco_0001_part_00.gz', 'r+'),
		))->get('ObjectURL');

		$s3Client->putObject(array(
			'Bucket' => $uploadParams['bucket'],
			'Key'    => $uploadParams['key'] . 'manifest',
			'Body'   => json_encode(array(
				'entries' => array(
					array(
						'url' => 's3://' . $uploadParams['bucket'] . '/' . $uploadParams['key'] . 'part001.gz',
					),
					array(
						'url' => 's3://' . $uploadParams['bucket'] . '/' . $uploadParams['key'] . 'part002.gz',
					)
				),
			)),
		))->get('ObjectURL');

		$headerFile = new CsvFile(__DIR__ . '/../_data/sliced/header.csv');
		$tableId = $this->_client->createTable($this->getTestBucketId(self::STAGE_IN, $backend), 'entries', $headerFile);
		$this->_client->writeTableAsyncDirect($tableId, array(
			'dataFileId' => $slicedFile['id'],
			'columns' => $headerFile->getHeader(),
			'delimiter' => '|',
			'enclosure' => '',
		));
	}

	/**
	 * @dataProvider backends
	 * @param $backend
	 */
	public function testSlicedImportSingleFile($backend)
	{
		$uploadOptions = new \Keboola\StorageApi\Options\FileUploadOptions();
		$uploadOptions
			->setFileName('languages_')
			->setIsSliced(true)
			->setIsEncrypted(false);
		$slicedFile = $this->_client->prepareFileUpload($uploadOptions);

		$uploadParams = $slicedFile['uploadParams'];
		$credentials = new Aws\Common\Credentials\Credentials(
			$uploadParams['credentials']['AccessKeyId'],
			$uploadParams['credentials']['SecretAccessKey'],
			$uploadParams['credentials']['SessionToken']
		);

		$s3Client = \Aws\S3\S3Client::factory(array('credentials' => $credentials));
		$part1URL = $s3Client->putObject(array(
			'Bucket' => $uploadParams['bucket'],
			'Key'    => $uploadParams['key'] . 'part001.csv',
			'Body'   => fopen(__DIR__ . '/../_data/languages.no-headers.csv', 'r+'),
		))->get('ObjectURL');

		$s3Client->putObject(array(
			'Bucket' => $uploadParams['bucket'],
			'Key'    => $uploadParams['key'] . 'manifest',
			'Body'   => json_encode(array(
				'entries' => array(
					array(
						'url' => 's3://' . $uploadParams['bucket'] . '/' . $uploadParams['key'] . 'part001.csv',
					),
				),
			)),
		))->get('ObjectURL');

		$tableId = $this->_client->createTable($this->getTestBucketId(self::STAGE_IN, $backend), 'entries', new CsvFile(__DIR__ . '/../_data/languages.csv'));
		$this->_client->deleteTableRows($tableId);
		$this->_client->writeTableAsyncDirect($tableId, array(
			'dataFileId' => $slicedFile['id'],
			'delimiter' => ',',
			'enclosure' => '"',
			'escapedBy' => '',
			'columns' => array('id', 'name'),
		));

		$this->assertLinesEqualsSorted(file_get_contents(__DIR__ . '/../_data/languages.csv'), $this->_client->exportTable($tableId, null, array(
			'format' => 'rfc',
		)), 'imported data comparsion');

		// incremental
		$this->_client->writeTableAsyncDirect($tableId,  array(
			'dataFileId' => $slicedFile['id'],
			'incremental' => true,
			'delimiter' => ',',
			'enclosure' => '"',
			'escapedBy' => '',
			'columns' => array('id', 'name'),
		));

		$data = file_get_contents(__DIR__ . '/../_data/languages.csv');
		$lines = explode("\n", $data);
		array_shift($lines);
		$data = $data . implode("\n", $lines);

		$this->assertLinesEqualsSorted($data, $this->_client->exportTable($tableId, null, array(
			'format' => 'rfc',
		)), 'imported data comparsion');
	}

	/**
	 * @dataProvider backends
	 * @param $backend
	 */
	public function testSlicedImportMissingManifest($backend)
	{
		$uploadOptions = new \Keboola\StorageApi\Options\FileUploadOptions();
		$uploadOptions
			->setFileName('entries_')
			->setIsEncrypted(false)
			->setIsSliced(true);
		$slicedFile = $this->_client->prepareFileUpload($uploadOptions);

		$tableId = $this->_client->createTable($this->getTestBucketId(self::STAGE_IN, $backend), 'entries', new CsvFile(__DIR__ . '/../_data/sliced/header.csv'));

		try {
			$this->_client->writeTableAsyncDirect($tableId, array(
				'dataFileId' => $slicedFile['id'],
				'withoutHeaders' => true,
				'delimiter' => '|',
				'enclosure' => '',
				'escapedBy' => '',
			));
			$this->fail('Exception should be thrown');
		} catch (\Keboola\StorageApi\ClientException $e) {
			$this->assertEquals('storage.manifestFileMissing', $e->getStringCode());
		}
	}

	public function testInvalidFilesInManifest()
	{
		$uploadOptions = new \Keboola\StorageApi\Options\FileUploadOptions();
		$uploadOptions
			->setFileName('entries_')
			->setIsEncrypted(false)
			->setIsSliced(true);
		$slicedFile = $this->_client->prepareFileUpload($uploadOptions);

		$tableId = $this->_client->createTable($this->getTestBucketId(), 'entries', new CsvFile(__DIR__ . '/../_data/sliced/header.csv'));

		$uploadParams = $slicedFile['uploadParams'];
		$credentials = new Aws\Common\Credentials\Credentials(
			$uploadParams['credentials']['AccessKeyId'],
			$uploadParams['credentials']['SecretAccessKey'],
			$uploadParams['credentials']['SessionToken']
		);

		$s3Client = \Aws\S3\S3Client::factory(array('credentials' => $credentials));
		$part1URL = $s3Client->putObject(array(
			'Bucket' => $uploadParams['bucket'],
			'Key'    => $uploadParams['key'] . 'part001.gz',
			'Body'   => fopen(__DIR__ . '/../_data/sliced/neco_0000_part_00.gz', 'r+'),
		))->get('ObjectURL');

		$s3Client->putObject(array(
			'Bucket' => $uploadParams['bucket'],
			'Key'    => $uploadParams['key'] . 'manifest',
			'Body'   => json_encode(array(
				'entries' => array(
					array(
						'url' => 's3://' . $uploadParams['bucket'] . '/' . $uploadParams['key'] . 'part001.gz',
					),
					array(
						'url' => 's3://' . $uploadParams['bucket'] . '/' . $uploadParams['key'] . 'part001.gzsome',
					)
				),
			)),
		))->get('ObjectURL');

		try {
			$this->_client->writeTableAsyncDirect($tableId, array(
				'dataFileId' => $slicedFile['id'],
				'columns' => array('id', 'added_manually', 'start', 'end', 'task_id', 'project_id'),
				'delimiter' => '|'
			));
			$this->fail('Exception should be thrown');
		} catch (\Keboola\StorageApi\ClientException $e) {
			$this->assertEquals('storage.importFileMissing', $e->getStringCode());
		}
	}

	/**
	 * @dataProvider backends
	 * @param $backend
	 */
	public function testUnauthorizedAccessInManifestFile($backend)
	{
		$tableId = $this->_client->createTable($this->getTestBucketId(self::STAGE_IN, $backend), 'entries', new CsvFile(__DIR__ . '/../_data/sliced/header.csv'));

		$uploadOptions = new \Keboola\StorageApi\Options\FileUploadOptions();
		$uploadOptions
			->setFileName('entries_')
			->setIsEncrypted(false)
			->setIsSliced(true);

		// First upload
		$slicedFile = $this->_client->prepareFileUpload($uploadOptions);

		$uploadParams = $slicedFile['uploadParams'];
		$credentials = new Aws\Common\Credentials\Credentials(
			$uploadParams['credentials']['AccessKeyId'],
			$uploadParams['credentials']['SecretAccessKey'],
			$uploadParams['credentials']['SessionToken']
		);

		$s3Client = \Aws\S3\S3Client::factory(array('credentials' => $credentials));
		$part1URL = $s3Client->putObject(array(
			'Bucket' => $uploadParams['bucket'],
			'Key'    => $uploadParams['key'] . 'part001.gz',
			'Body'   => fopen(__DIR__ . '/../_data/escaping.csv', 'r+'),
		))->get('ObjectURL');


		// Second upload
		$slicedFile = $this->_client->prepareFileUpload($uploadOptions);
		$uploadParams = $slicedFile['uploadParams'];
		$credentials = new Aws\Common\Credentials\Credentials(
			$uploadParams['credentials']['AccessKeyId'],
			$uploadParams['credentials']['SecretAccessKey'],
			$uploadParams['credentials']['SessionToken']
		);
		$s3Client = \Aws\S3\S3Client::factory(array('credentials' => $credentials));

		$s3Client->putObject(array(
			'Bucket' => $uploadParams['bucket'],
			'Key'    => $uploadParams['key'] . 'manifest',
			'Body'   => json_encode(array(
				'entries' => array(
					array(
						'url' => $part1URL,
					),
				),
			)),
		))->get('ObjectURL');

		try {
			$this->_client->writeTableAsyncDirect($tableId, array(
				'dataFileId' => $slicedFile['id'],
				'withoutHeaders' => true,
				'delimiter' => '|',
			));
			$this->fail('Exception should be thrown');
		} catch (\Keboola\StorageApi\ClientException $e) {
			$this->assertEquals('storage.unauthorizedAccess', $e->getStringCode());
		}
	}

}