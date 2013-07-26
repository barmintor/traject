# Encoding: UTF-8

require 'test_helper'

require 'traject'
require 'traject/indexer'
require 'traject/marc4j_reader'

require 'marc'

describe "Marc4JReader" do
  it "reads Marc binary" do
    file = File.new(support_file_path("test_data.utf8.mrc"))
    settings = Traject::Indexer::Settings.new() # binary type is default
    reader = Traject::Marc4JReader.new(file, settings)

    array = reader.to_a

    assert_equal 30, array.length
    first = array.first

    assert_kind_of MARC::Record, first
    assert first['245']['a'].encoding.name, "UTF-8"
  end

  it "reads Marc binary in Marc8 encoding" do
    file = File.new(support_file_path("one-marc8.mrc"))
    settings = Traject::Indexer::Settings.new("marc4j_reader.source_encoding" => "MARC8")
    reader = Traject::Marc4JReader.new(file, settings)

    array = reader.to_a

    assert_length 1, array


    assert_kind_of MARC::Record, array.first
    a245a = array.first['245']['a']

    assert a245a.encoding.name, "UTF-8"
    assert a245a.valid_encoding?
    # marc4j converts to denormalized unicode, bah. Although
    # it's legal, it probably looks weird as a string literal
    # below, depending on your editor. 
    assert_equal "Por uma outra globalização :", a245a
  end


  it "reads XML" do
    file = File.new(support_file_path "test_data.utf8.marc.xml")
    settings = Traject::Indexer::Settings.new("marc_source.type" => "xml")
    reader = Traject::Marc4JReader.new(file, settings)

    array = reader.to_a

    assert_equal 30, array.length

    first = array.first

    assert_kind_of MARC::Record, first
    assert first['245']['a'].encoding.name, "UTF-8"
    assert_equal "Fikr-i Ayāz /", first['245']['a']
  end
end