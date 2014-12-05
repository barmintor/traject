# encoding: UTF-8

require 'test_helper'
require 'traject/solr_json_writer'

require 'vcr'

VCR.configure do |c|
  c.default_cassette_options = { :record => :new_episodes, :erb => true }
  c.cassette_library_dir = 'fixtures/vcr_cassettes'
  c.hook_into :webmock
  c.ignore_localhost = false
  c.debug_logger     = $STDERR
end

describe "test a simple write" do

  def next_context
    c             = Traject::Indexer::Context.new
    c.output_hash = {'id' => [@lastid += 1], 'name_t' => ['Bill']}
    c
  end

  before do
    VCR.use_cassette('solr_json_writer') do
      @writer              = Traject::SolrJsonWriter.new({'solr.url' => "http://localhost:8983/solr/core1"})
      @context             = Traject::Indexer::Context.new
      @context.output_hash = {'id' => [1], 'name_t' => ['Bill']}
      @lastid              = 0
    end

  end

  it "does a basic write" do
    VCR.use_cassette('solr_json_writer_goodrecord') do
      @writer.put(next_context)
      @writer.close
      @writer.commit

      @writer.skipped_record_count.must_equal 0
    end
  end

  it "fails to index a bad record" do
    VCR.use_cassette('solr_json_writer_badrecord') do

      @writer.put(next_context)
      c                   = next_context
      c.output_hash['id'] = [1, 2] # illegal; two ids
      @writer.put(c)
      @writer.close
      @writer.skipped_record_count.must_equal 1
    end
  end

  it "batches correctly" do
    VCR.use_cassette('solr_json_writer') do

      @writer.batch_size = 10
      9.times do |i|
        @writer.put next_context
      end
      @writer.batched_queue.size.must_equal 9

      @writer.put next_context
      @writer.batched_queue.size.must_equal 0 # batch sent
    end

  end

end
