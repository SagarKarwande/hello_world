class Contact < ActiveRecord::Base

  extend ElasticsearchExtension
  extend ContactExtension

  extend DataWrapper
  extend PersonalContactMatch
  
  include ContactSearch
  include LocationWrapper
  include Linkedin::ImageOnS3
  include ContactWrapper
  include Linkedin::NormalizeName
  include ContactElasticsearch

  DATA_REFRESH_AFTER = 45.days
  DATA_REFRESH_AFTER_DAYS = 45
  VALID_EMAIL_REGEX = /\A([\w+\-]\.?)+@[a-z\d\-]+(\.[a-z]+)*\.[a-z]+\z/i
  REFRESH_AFTER = 30

  validates :first_name, :last_name, :iv_id, :popularity,  presence: true
  validates :first_name, :last_name, :email, length: {maximum: 60}
  validates :company_name, length: {maximum: 120}
  validates :phone, length: {maximum: 220}
  validates :twitter_username, length: {maximum: 150}
  validates :md5_linkedin_url, uniqueness: true, allow_blank: true, on: :create

  has_many :educations, dependent: :destroy

  has_many :contact_groups, dependent: :destroy
  has_many :groups, through: :contact_groups

  has_many :contact_skills, dependent: :destroy
  has_many :skills, through: :contact_skills

  has_many :companies, through: :positions
  has_many :positions, dependent: :destroy

  has_many :matched_attendees
  has_many :meetings, through: :matched_attendees

  has_many :position_departments
  has_many :position_seniorities

  has_many :discovered_contacts, dependent: :destroy
  has_many :lead_discoveries, through: :discovered_contacts

  has_many :contact_watchlist_contacts, dependent: :destroy
  has_many :contact_watchlists, through: :contact_watchlist_contacts

  has_many :leads_for_enrichment, dependent: :destroy
  has_many :lead_enrichments, through: :leads_for_enrichment

  has_many :contact_social_profiles, dependent: :destroy

  has_many :oxy_audits, dependent: :destroy, class_name: 'OxyContactAudit'

  has_one :profile_refresh, -> { order(id: :desc) }, class: ContactProfileRefresh

  serialize :company_agg_data, Array
  serialize :position_agg_data, Array

  scope :by_name, -> { order("lower(first_name), lower(last_name)") }
  scope :popular, -> { order(:popularity) }
  scope :with_verified_emails, -> { where(email_status: ['Verified', 'Guaranteed']) }
  scope :with_verified_pattern_emails, -> { where(email_status: 'Verified Pattern') }

  default_scope { where(Contact.default_scope_query)}

  mount_uploader :ds_logo, AvatarUploader

  before_create :map_location, :update_image
  before_save :clean_first_and_last_name
  before_update :map_location, if: :location_changed?
  before_update  :update_image, if: :image_url_changed?
  #TODO Removing EGS/EVS on contact update.
  # before_update :email_validation
  after_save :set_email_status_order, :update_last_verified_flag
  before_validation :save_sanitized_li_url_data, :save_md5_sales_navigator_li_url, :assign_normalized_name

  VALID_UPDATION_CHANGE_KEYS = {
    company: :company,
    title: :title,
    location: :location,
    skills: :skills,
    updation_time: :updation_time
  }

  LINKEDIN_EXCEPTIONS = [
    "login required", "no linkedin data", "invalid linkedin url", 
    "no linkedin match", "no google data"
  ]

  DEFAULT_IMAGE = "default_contact_image.png"

  CHANGE_TRACKED_FOR = {
    company: 'company', 
    title: 'title',
    location: 'location',
    skills: 'skills'
  }

  def update_test_data(test_data_params)
    return unless self.test_data
    if test_data_params[:company_name].present?
      self.update_attributes(company_name: test_data_params[:company_name])
      pos = self.positions.first
      if pos.present?
        com = pos.company
        com.update_attributes(name: test_data_params[:company_name]) if com.present?
      end
    end

    if test_data_params[:title].present?
      self.update_attributes(title: test_data_params[:title])
      pos = self.positions.first
      pos.update_attributes(name: test_data_params[:title]) if pos.present?
    end

    if test_data_params[:location].present?
      self.update_attributes(location: test_data_params[:location])
    end

    if test_data_params[:linkedin_url].present?
      self.update_attributes(linkedin_url: test_data_params[:linkedin_url])
    end
  end

  def select_fields(user = nil, position: nil, criteria: nil)
    contact_info = self.as_json({
      only: [
        :id, :first_name, :last_name, :last_name, :company_name, :title, :email, :phone, :industry,
        :linkedin_url, :updated_at, :li_updated_at, :relevance_score, :email_status
      ],
      methods: [
        :logo_url,
        :location_area,
        :valid_email
      ]
    }.reject{ |k,v| v.blank? } )
    contact_info.merge!(is_watchlisted: is_watchlisted?(user)) if user.present?
    contact_info.merge!(headline: headline(position: position, criteria: criteria), 
                        company: company(position: position, criteria: criteria),
                        relevant_title: relevant_title(position: position, criteria: criteria)
                       )
    contact_info
  end

  def position_and_contact_agg_data
    self.update_attributes(company_agg_data: contact_company_agg_data, position_agg_data: contact_position_agg_data)
  end

  def process_pending_queues
    RefreshContact.process_pending_queues(self.linkedin_url, self.id)
    SearchContact.process_pending_queues(self.linkedin_url, self.id)
    BlockedContact.process_pending_queues(self.linkedin_url, self.id)
    ContactProfileRefresh.process_pending_queues(self.linkedin_url, self.id) 
  end

  def update_email_and_email_status(email, email_status)
    if self.valid_email
      self.update_columns(email: self.email, email_status: 'Verified', egs_updated_at: DateTime.now)
    elsif self.email.present? && self.email != email
      self.update_columns(
        email: email,
        old_email: self.email,
        email_status: email_status,
        email_validation_remark: nil,
        evs_updated_at: nil,
        egs_updated_at: DateTime.now
      )
    else
      self.update_columns(email: email, email_status: email_status, egs_updated_at: DateTime.now)
    end
    ::V0::ElasticsearchService.new(self).index_object
  end

  def country
    Country.find_by_id(self.country_id).try(:name)
  end
  
  def search_data
    position = get_position_for_headline
    if position.present?
      "#{first_name} #{last_name} #{position.company_name}"
    else
      "#{first_name} #{last_name} #{company_name}"
    end
  end

  def search_on_name
    "#{first_name} #{last_name}"
  end

  def get_data_from_iv
    return true
=begin
    return if (self.from_iv.eql?(true) || self.iv_timestamp.present?)
    iv_contact = MultiSource::IvContact.new
    iv_contact.iv_contact_data(self.iv_id)
    iv_contact = iv_contact.save_contacts_details(self)
=end
  end

  def get_data_from_linkedin(is_sleep: true, queue: 0)
    return if LINKEDIN_EXCEPTIONS.include?(self.remark)
    return unless refresh_linkedin_data?
    sleep rand(SLEEP_RANGE).seconds if is_sleep
    linkedin = Fetch::LinkedinContact.new(self, queue: queue)
    linkedin.fetch
  end

  def validate_email
    ##
    # TODO: EGS/EVS enable this if we want to run EGS/EVS
    # a. Contact profile click
    # b. 'Target Contacts' list gets updated/created
    # But we need position to use new EGS/EVS

    # return if self.email_validation_remark.present? || self.from_csv.present?
    # args = {
    #   contact_ids: self.id,
    #   queue: 0,
    # }
    # EmailValidationWorker.perform_async(args)
  end

  def profile user
    profile_json = self.as_json({
      only: [ 
        :id, :first_name, :last_name, :email, :phone, :linkedin_url,
        :facebook_url, :twitter_url, :twitter_username, :location, :industry,
        :li_updated_at, :updated_at, :email_status, :from_linkedin, :age_range, :gender
      ],
      methods: [
        :company, :bio, :current_position, :past_positions, :other_current_positions, 
        :education, :actions, :headline, :recent_visits, :logo_url, :valid_email,
        :is_fresh, :profile_refresh_status, :is_refreshed_in_update_interval,
        :is_partially_enriched, :is_enriched, :is_refresh_for_enrich_only, :additional_social_profiles,
        :contact_info, :personal_email_data, :current_companies_and_positions,
        :last_refresh_process_data
      ]
    })

    profile_json.merge!({ 
      skills: skills_info, 
      groups: groups_info, 
      is_watchlisted: is_watchlisted?(user)
    }).merge!(personal_contact_info(user)).merge!(currently_running_refresh_process(user)).merge!(invalid_linkedin_url: invalid_linkedin_url?)
    # Note: we reject null fields from profile JSON
    profile_json.reject{|k, v| v.blank? && !v.eql?(false) }
  end

  def invalid_linkedin_url?
    self.invalid_li_profile
  end

  def current_companies_and_positions
    positions = self.positions.where('(deleted_from_li IS NULL OR deleted_from_li IS FALSE) AND is_current IS TRUE').order('start_date DESC')
    handled_companies = []
    final_companies = []
    positions.each do |pos|
      unless handled_companies.include?(pos.company_name)
        currently_running_process = pos.running_enrichment_process
        final_companies << {
          company_name: pos.company_name,
          company_id: pos.company_id,
          is_current: pos.is_current,
          position_id: pos.id,
          corporate_phone_data: pos.get_corporate_phone(currently_running_process: currently_running_process),
          direct_phone_data: pos.get_direct_phone(currently_running_process: currently_running_process),
          mobile_number_data: pos.get_mobile_number(currently_running_process: currently_running_process),
          work_email_data: pos.get_work_email(currently_running_process: currently_running_process),
          cma_data: pos.get_cma(currently_running_process: currently_running_process),
          running_enrichment_process_data: pos.get_running_enrichment_process_data(currently_running_process: currently_running_process)
        }
        handled_companies << pos.company_name
      end
    end
    final_companies
  end

  def enrichment_process_for_position?(currently_running_process, position)
    return false if currently_running_process.blank? || currently_running_process.position_id.blank? || position.blank?
    currently_running_process.position_id.eql?(position.id)
  end

  def personal_email_data
    enrichment_process = running_enrichment_process
    personal_email_status = enrichment_process.present? ? status_for_a_requested_attribute(enrichment_process, "personal_email") : ContactProfileRefresh::REQUESTED_ATTRIBUTES_STATUSES[:not_required]
    { 
      personal_email: self.personal_email,
      personal_email_last_update: self.personal_email_last_update,
      personal_email_status: self.personal_email_status,
      personal_email_verified: self.personal_email_verified,
      running_process_status: personal_email_status,
      is_fresh: (self.personal_email_last_update && self.personal_email_last_update > 4.weeks.ago)
    }
  end

  def running_enrichment_process
    processed_search_state = ContactProfileRefresh.search_states[:processed]
    ContactProfileRefresh.where(contact_id: self.id).where.not(search_state: processed_search_state).order('id').last
  end

  def last_refresh_process_data
    cpr = ContactProfileRefresh.where(contact_id: self.id).last
    { last_refresh_process_time: cpr.try(:created_at) }
  end

  def currently_running_refresh_process(user)
    processed_search_state = ContactProfileRefresh.search_states[:processed]
    currently_running_refresh_process = ContactProfileRefresh.where(contact_id: self.id).where.not(search_state: processed_search_state)
    # currently_running_refresh_process = currently_running_refresh_process.where(user_id: user.id) if user.present?
    currently_running_refresh_process = currently_running_refresh_process.order('id DESC').first
    running_enrichment_process_data = get_running_enrichment_process_data(currently_running_refresh_process)
    { currently_running_refresh_process: running_enrichment_process_data }
  end

  def get_running_enrichment_process_data(currently_running_process)
    return nil if currently_running_process.blank?
    progress_statuses = ContactProfileRefresh::PROGRESS.map do |progress|
      statuses = progress[:value].map do |value|
        status_for_a_requested_attribute(currently_running_process, value)
      end
      { name: progress[:name], status: overall_status(statuses) }
    end
    { progress_statuses: progress_statuses, status: overall_status(progress_statuses.map{ |status| status[:status] }), started_time: currently_running_process.created_at }
  end

  def overall_status(statuses)
    if statuses.all? { |str| str.eql?(:not_required) }
      :not_required
    elsif statuses.all? { |str| str.eql?(:completed) }
      :completed
    elsif statuses.any? { |str| str.eql?(:completed) }
      if statuses.any? { |str| str.eql?(:in_progress) }
        :in_progress
      else
        :completed
      end
    elsif statuses.any? { |str| str.eql?(:in_progress) }
      :in_progress
    else
      :not_started
    end
  end

  def status_for_a_requested_attribute(currently_running_process, value)
    attribute = currently_running_process.requested_attributes.find{ |attr| attr[:name] == value || attr['name'] == value }
    if attribute.blank?
      ContactProfileRefresh::REQUESTED_ATTRIBUTES_STATUSES[:not_required]
    else
      if currently_running_process.contact_profile_refresh_request.bad_data?
        return ContactProfileRefresh::REQUESTED_ATTRIBUTES_STATUSES[:wrong] if attribute[:status].eql?(ContactProfileRefresh::REQUESTED_ATTRIBUTES_STATUSES[:not_started])
      end
      attribute[:status]
    end
  end

  def contact_info
    return [] unless is_fresh
    return contact_info_for_current_positions if current_positions_present?
    contact_info_for_past_positions
  end

  def contact_info_for_current_positions
    # Query to select data from only one position for each company id for current positions ordered by most recent
    select = "position_id, direct_phone, direct_phone_verified, mobile_number, corporate_phone, corporate_phone_verified, email,
    email_status, phone, company_id, company_name, enrichment_level"
    query = <<-QUERY
      SELECT #{select} FROM (
        SELECT positions.id as position_id, positions.direct_phone, positions.enrichment_level, 
        CASE WHEN positions.direct_phone_verified = TRUE THEN 'true' else 'false' end as direct_phone_verified, 
        positions.mobile_number, companies.corporate_phone, 
        CASE WHEN companies.corporate_phone_verified = TRUE THEN 'true' else 'false' end as corporate_phone_verified, 
        positions.email, positions.email_status, contacts.phone, companies.id as company_id, 
        companies.name as company_name, row_number() 
        OVER ( PARTITION BY positions.company_id ORDER BY positions.start_date DESC )
        FROM contacts, positions INNER JOIN companies ON companies.id = positions.company_id 
        WHERE positions.contact_id = contacts.id 
        AND contacts.id = #{self.id} 
        AND positions.is_current = true
        ORDER BY start_date desc
      ) tmp
      WHERE tmp.row_number <= 1
    QUERY
    ActiveRecord::Base.connection.exec_query(query).as_json
  end

  def contact_info_for_past_positions
    select = "positions.id as position_id, positions.direct_phone, positions.enrichment_level, 
    CASE WHEN positions.direct_phone_verified = TRUE THEN 'true' else 'false' end as
    direct_phone_verified, positions.mobile_number, companies.corporate_phone, CASE WHEN
    companies.corporate_phone_verified = TRUE THEN 'true' else 'false' end as corporate_phone_verified, 
    positions.email, positions.email_status, contacts.phone, companies.id as company_id, companies.name as company_name"
    order = 'positions.created_at DESC'
    hsh = { contact_id: self.id, is_current: false }
    Position.joins(:company, :contact).where(hsh).order(order).select(select).limit(1).as_json
  end

  def current_positions_present?
    self.positions.where(is_current: true).count > 0
  end

  def corporate_phones
    select = 'companies.corporate_phone, companies.corporate_phone_verified'
    phones = self.positions.joins(:company).where(is_current: true).select(select).as_json
    phones.reject!{ |phone| phone["corporate_phone"].blank? }
    phones
  end

  def direct_phones
    phones = self.positions.where(is_current: true).select('direct_phone, direct_phone_verified').as_json
    phones.reject!{ |phone| phone["direct_phone"].blank? }
    phones
  end

  def mobile_numbers
    phones = self.positions.where(is_current: true).select('mobile_number').as_json
    phones.reject!{ |phone| phone["mobile_number"].blank? }
    phones
  end

  def profile_refresh_status
    profile_refresh.status if profile_refresh.present?
  end

  def is_refresh_for_enrich_only
    profile_refresh.is_enrich_only if profile_refresh.present?
  end

  def is_refreshed_in_update_interval
    profile_refresh.is_refreshed_in_update_interval? if profile_refresh.present?
  end

  def is_fresh
    (li_updated_at && li_updated_at > 4.weeks.ago) && from_linkedin?
  end

  def is_enriched
    position = get_position_for_headline

    position.present? &&
      position.email.present? &&
      ['Guaranteed', 'Verified', 'Verified Pattern'].include?(position.email_status) &&
      phone.present? &&
      phone_verification_remark.present? &&
      phone_verification_remark == 'PVS,Success'
  end

  def is_partially_enriched
    position = get_position_for_headline

    (
      position.present? &&
        position.email.present? &&
        ['Guaranteed', 'Verified', 'Verified Pattern'].include?(position.email_status)
    ) || (
      phone.present? &&
        phone_verification_remark.present? &&
        phone_verification_remark == 'PVS,Success'
    )
  end

  def actions
    ["Call", "Email", "Tweet"]
  end

  def valid_email
    self.email_validation_remark.present? && self.email_validation_remark.include?('Success')
  end
  
  def json_for_watchlist(position: nil)
    self.select_fields(position: position).merge!(self.as_json(only: [ :email, :phone ]))
  end

=begin
  def json_for_my_contact_watchlist(user)
    personal_contact = user.personal_contacts.where(contact: self).try(:first)
    return json_for_watchlist if personal_contact.blank? 
    personal_info = {
      id: personal_contact.id,
      email: personal_contact.email,
      phone: personal_contact.phone
    }
    json_for_watchlist.merge(personal_info: personal_info)
  end
=end

  def headline(truncate: true, criteria: nil, position: nil)
    position = relevant_position(criteria: criteria, position: position)
    if position.nil?
      # truncated = truncate ? self.title.truncate(40) : self.title
      truncated = self.title
      dispaly_title = self.title.blank? ? 'Works' : truncated
      dispaly_title + ' at ' + self.company_name.to_s
    else
      position.headline(truncate: truncate)
    end
  end

  def relevant_title(criteria: nil, position: nil)
    position = relevant_position(criteria: criteria, position: position)
    position.try(:title) || title
  end

  def relevant_position(criteria: nil, position: nil)
    position = position || Position.valid_position_by_criteria(self, criteria) || get_position_for_headline
  end

  def company(position: nil, criteria: nil)
    # If contact is not found on linkedin we send company object where company_id is not null 
    # Note: if contact not found on linkedin then company_id is null in position table as we 
    # currently don't match linkedin position companies with IV companies 
    position = relevant_position(criteria: criteria, position: position)
    return { name: self.company_name } unless position
    return position.company.base_info.reject{ |k,v| v.blank? } if position.company
    return { name: position.company_name } if position
  end

  def groups_info
    self.groups.limit(10).as_json({ only: [:id, :name, :link, :image_url ] })
  end

  def skills_info
    self.skills.collect(&:title).uniq
  end

  def positions_company_names
    positions.pluck(:company_name).compact unless positions.blank?
  end

  def current_position
    # If contact is found not found on linkedin and data is collected only by 
    # IV api then we send current postion provided by IV api
    if self.li_overwritten_at? or self.from_linkedin
      @first_current_position = current_position_from_linkedin
      @all_c_positions = self.positions.where(is_current: true, from_linkedin: true).
        order("start_date desc")
      return @first_current_position.position_data(send_email: true) if @first_current_position.present?
      nil
    end
  end

  def current_position_from_linkedin
    self.positions.where(is_current: true, from_linkedin: true).
      order("start_date DESC").first || self.positions.where(is_current: true, from_linkedin: true).
      order("start_date desc").first
  end

  def past_positions
    self.current_position
    position_records = self.positions.uniq unless self.from_linkedin
    position_records = self.positions.where(is_current: false, from_linkedin: true, deleted_from_li: false).order("end_date desc").uniq
    return get_position_json(position_records, send_email: true) if @first_current_position.present?
    get_position_json(position_records, send_email: true)
  end

  def other_current_positions
    self.current_position
    if @all_c_positions
      @all_c_positions = @all_c_positions.reject{|pp| pp.id == @first_current_position.id }
      get_position_json(@all_c_positions, send_email: true)
    end
  end

  def get_position_json(position_records, send_email: false)
    position_json = position_records.map { |position_record| position_record.position_data(send_email: send_email) }
    position_json.each{|l| l.reject!{|k,v| v.blank?}}.uniq if position_json    
  end

  def bio
    self.linkedin_summary.presence || self.description
  end

  def get_position_for_headline
    current_position_from_linkedin || self.positions.first
  end

  def name
    return nil if self.first_name.blank? && self.last_name.blank?
    [self.first_name, self.last_name].compact.join(' ')
  end

  def education
    education_json = self.educations.
      select("distinct(major), university, degree, start_date, end_date").
      as_json({
        only: [ :university, :major, :degree, :start_date, :end_date ]
      })
      education_json.each{|l| l.reject!{|k,v| v.blank?}}.uniq if education_json
  end

  def company_news
    com = self.companies_for_news
    return [] if com.blank?
    c_news = []
    if com.kind_of? Array
      com.each do |company|
        c_news << company.news(Company::RECENT_NEWS_PAGE_NO, Company::RECENT_NEWS_SIZE)[0] unless company.blank?
      end
    else
      c_news << com.news(Company::RECENT_NEWS_PAGE_NO, Company::RECENT_NEWS_SIZE)[0]
    end
    c_news.flatten
  end

  def recent_visits
    User::TwitterAccount.recent_visits(self.twitter_username)
  end

  def upcoming_meetings(user_id)
    upcoming_meetings = query_meetings(">=", "upcoming", user_id)
    upcoming_meetings.collect(&:meeting_json)
  end

  def meetings_json(user_id)
    { upcoming: upcoming_meetings(user_id), recent: recent_meetings(user_id) }     
  end

  def recent_meetings(user_id)
    recent_meetings = query_meetings("<=", "recent", user_id)
    recent_meetings.collect(&:meeting_json)
  end

  def query_meetings(operator, scope_name, user_id)
    self.meetings.send(scope_name).
      where("start_time #{operator} ? and user_id = ?", DateTime.now.utc, user_id).limit(3)
  end

  def all_current_positions  
    self.positions.where(is_current: true, from_linkedin: true).where.
      not(company_name: nil).order("start_date desc nulls last")
  end

  def companies_for_news
    if li_overwritten_at? or from_linkedin
      com = companies_from_linkedin
    else
      com = companies_from_iv
    end
    com
  end

  def companies_from_linkedin
    com = positions.where(is_current: true, from_linkedin: true).order("start_date desc").collect(&:company)
    com = positions.where(from_linkedin: true).where.not(start_date: nil).order("start_date desc").
      first.try(:company) if com.blank?
    com
  end

  def companies_from_iv
    self.positions.order(:id).first.try(:company)
  end

  def is_watchlisted? user
    user.contacts.find_by(id: self.id).present?
  end

  #Kiran : Used in basic search for es
  def self.dynamic_field_for_es(user, id, criteria: {})
    data = {}
    position = nil
    contact = user.present? ? user.contacts.find_by(id: id) : nil
    data.merge!({is_watchlisted: contact.present? ? true : false})
    contact = Contact.find_by_id(id) if contact.blank?
    position = contact.relevant_position(criteria: criteria)
    email =  position.present? ? position.email : nil
    email_status =  position.present? ? position.email_status : nil
    data.merge!({ email: email, email_status: email_status, is_fresh: contact.is_fresh })
    
    headline_value = contact.present? ? contact.headline(criteria: criteria, position: position) : nil
    data.merge!(headline: headline_value) 
    data
  end

  def personal_contact_info user
    con = user.personal_contacts.where(contact: self).try(:first)
    con.blank? ? { is_in_personal_contact: false } : { 
      is_in_personal_contact: true, 
      personal_phone: con.phone, 
      personal_email: con.email
    }
  end

  def refresh_linkedin_data?
    return true if self.li_updated_at.blank?
    self.li_updated_at <= DateTime.now - DATA_REFRESH_AFTER
  end

  def email_validation
    #TODO: Disabling EGS/EVS on contact update
    # process_email_validation if self.company_name_changed? && self.popularity != 0
  end

  def process_email_validation
    updated_old_email = self.email.eql?(self.old_email) ? self.old_email : self.email
    self.update_columns(email_validation_remark: nil, email: nil, old_email: updated_old_email,
                        egs_updated_at: nil, evs_updated_at: nil, email_status: nil)
    ::V0::ElasticsearchService.new(self).index_object
    self.trigger_egs
    EmailValidationWorker.perform_async({ contact_ids: self.id, queue: 0 })
  end

  def json_for_lead(user = nil)
    con = json_for_watchlist
    con.merge!(is_watchlisted: is_watchlisted?(user)) if user.present?
    con
  end

  def self.export

    start_time = DateTime.now
    p "Started at: #{start_time}"
    csv_path = "#{Rails.root}/tmp/"

    headers = [
      "Company", "Contact Id", "First Name", "Last Name", "Title", "Location", "industry",
      "linkedIn URL", "Skills", "Email", "Email Status", "Phone"
    ]

    @contact_csv = csv_path + "test_export_time.csv"
    @lead_discovery = LeadDiscovery.find(47)
    CSV.open(@contact_csv, "wb") do |csv|
      csv << headers
      @lead_discovery.contacts.includes(:skills).includes(:positions).find_each do |con|
        csv << con.contact_attrs_to_export
      end
    end

    end_time = DateTime.now
    p "Ended at #{end_time}"
    p "Total time #{end_time.to_i - start_time.to_i}"
  end

  def save_md5_sales_navigator_li_url
    return true if self.sales_navigator_li_url.blank?
    self.md5_sales_navigator_li_url =  Digest::MD5.hexdigest(self.sales_navigator_li_url)
  end

  def ce_linkedin_res(current_user, append_watchlist_ids: false)
    data = {
      id: id, 
      name: self.name,
      linkedin_url: self.linkedin_url,
      li_updated_at: li_updated_at, 
      completeness_indicator: completeness_indicator, 
      is_watchlisted: is_watchlisted?(current_user),
      from_linkedin: from_linkedin
    }
    if append_watchlist_ids
      user_watchlist_ids = current_user.contact_watchlists.pluck(:id)
      watchlist_ids = ContactWatchlistContact.where(contact_watchlist_id: user_watchlist_ids, contact_id: self.id).pluck(:contact_watchlist_id)
      data.merge!({contact_watchlist_ids: watchlist_ids})
    end
    return data
  end

  def self.global_contact_with_li_url(li_url: nil, sales_li_url: nil)
    if li_url.present?
      li_url = li_url.strip.chomp('/')
      md5_linkedin_url = Digest::MD5.hexdigest(li_url)
      contact = Contact.find_by(md5_linkedin_url: md5_linkedin_url)
      return contact if contact.present?
    end

    if sales_li_url.present?
      sales_li_url = sales_li_url.strip.chomp('/')
      md5_linkedin_url = Digest::MD5.hexdigest(sales_li_url)
      contact = Contact.find_by(md5_sales_navigator_li_url: md5_linkedin_url)
      return contact if contact.present?
    end
    return nil
  end

  def self.set_location
    Contact.where.not(location: nil).where("location like ?", "%Canada%").
      where("location like ?", "%,%").find_each do |contact|
      contact.save(validate: false)
    end
  end

  def assign_normalized_name
    f_name = normalized_string(self.first_name)
    self.normalized_first_name = f_name unless self.normalized_first_name.eql?(f_name)
    l_name = normalized_string(self.last_name)
    self.normalized_last_name = l_name unless self.normalized_last_name.eql?(l_name)
    return true
  end

  def set_email_status_order
    if self.email_status
      self.update_column(:email_status_order, EMAIL_STATUS_ORDER[self.email_status])
      ::V0::ElasticsearchService.new(self).index_object
    end
  end

  def city_name
    City.find_by(id: city_id).try(:name)
  end

  def country_name
    Country.find_by(id: country_id).try(:name)
  end

  def state_name
    State.find_by(id: state_id).try(:name)
  end

  def relevant_position_by_company(company_id: nil, company_name: nil)
    return nil if company_id.blank? && company_name.blank?
    position = self.positions.where(company_id: company_id).order('start_date desc').first if company_id.present?
    return position if position.present?
    position = self.positions.where("company_name ilike (?)", "%#{company_name}%").order('start_date desc').first if company_name.present?
    position.present? ? position : nil
  end

  def add_social_url_into_table(type_name, url)
    return if type_name.blank? || url.blank?
    type_name = type_name.downcase
    add_li_url(url) and return if type_name.eql?('linkedin') && self.linkedin_url.blank?
    self.update_attributes({"#{type_name}_url" => url}) if ['twitter', 'facebook'].include?(type_name)
  end

  def add_li_url(li_url)
    md5_li_url = Digest::MD5.hexdigest(li_url)
    self.update_attributes(linkedin_url: li_url, md5_linkedin_url: md5_li_url)
  end

  def additional_social_profiles
    entities_in_contacts_table = ['twitter', 'facebook', 'linkedin']
    json_data = { only: [:entity_name, :entity_url] }
    order = 'entity_name'
    query = 'lower(entity_name) IN (?)'
    self.contact_social_profiles.where.not(query, entities_in_contacts_table).order(order).as_json(json_data)
  end

  def consume_contact_info_credits_for_user(user)
    return unless self.is_fresh # Do not consume any credit if the contact is not fresh
    self.positions.where(is_current: true).each do |pos|
      credits_consumed = user.update_subscription_position_history(pos.id, ENRICHMENT_LEVELS[:contact_profile], position: pos)
      user.add_to_credit_usage_history(credits: credits_consumed, type: CreditHistory::TYPES[:contact]) if credits_consumed > 0.0
    end
  end

  def find_contact_locations
    {
      city_id: self.city_id,
      state_id: self.state_id,
      country_id: self.country_id,
      city: self.city_name,
      state: self.state_name,
      country: self.country_name
    }
  end

  def mark_incomplete
    return if is_incomplete
    update_columns(is_incomplete: true)
    Contact.send_incomplete_contact_email_notifications if Contact.incomplete_contact_threshold_reached?
  end

  def has_incomplete_positions?
    positions.where(is_incomplete: true).present?
  end

  def update_incompletion_status_based_on_incomplete_positions
    return if is_incomplete
    mark_incomplete if has_incomplete_positions?
  end

  def contact_position_agg_data
    Position.includes(:company).where(contact_id: self.id).map do |position|
      work_duration = months_from_start_to_end_date(position.start_date, position.end_date)
      {
        id: position.id,
        start_date: position.start_date,
        end_date: position.end_date,
        is_current: position.is_current,
        company_id: position.company_id,
        company_name: position.company.try(:name),
        title: position.title,
        duration: work_duration
      }
    end
  end

  def contact_company_agg_data
    company_ids = self.positions.pluck('company_id').uniq
    company_data = []
    Company.where(id: company_ids).find_each do |company_object|
      company_data << company_tenure_agg_data(company_object)
    end
    company_data
  end

  def company_tenure_agg_data(company_object, duration_from_current_date: false)
    position_objects = self.positions.where(company_id: company_object.id).order('positions.start_date')
    current_company = false
    position_objects.each { |position_object| current_company = true if position_object.is_current }
    start_date = position_objects.first.start_date
    end_date = current_company ? (duration_from_current_date ? Date.today : nil) : position_objects.last.end_date
    work_duration = months_from_start_to_end_date(start_date, end_date)
    {
      id: company_object.id,
      name: company_object.name,
      is_current: current_company,
      start_date: start_date,
      end_date: end_date,
      duration: work_duration
    }
  end

  def months_from_start_to_end_date(start_date, end_date)
    return nil if start_date.blank? || end_date.blank?
    work_duration = (end_date.year * 12 + end_date.month) - (start_date.year * 12 + start_date.month)
    return nil if work_duration < 0
    work_duration + 1
  end

  def past_company_names
    company_ids = self.positions.where(is_current: [false, nil]).pluck('company_id')
    company_names = Company.where(id: company_ids).pluck('name')
    return nil if company_names.blank?
    company_names.join(', ')
  end

  def tenure_at_current_company
    latest_current_position = self.positions.where(is_current: true).order('start_date').last
    return nil if latest_current_position.blank? || latest_current_position.company.blank?
    lastest_current_company = latest_current_position.company
    current_company_agg_data = company_tenure_agg_data(lastest_current_company, duration_from_current_date: true)
    work_duration = current_company_agg_data[:duration]
    return nil if work_duration.blank?
    return "#{work_duration} Months" if work_duration < 12
    "#{work_duration / 12} Years, #{work_duration % 12} Months"
  end

  def tenure_at_current_position
    latest_current_position = self.positions.where(is_current: true).order('start_date').last
    return nil if latest_current_position.blank?
    work_duration = months_from_start_to_end_date(latest_current_position.start_date, Date.today)
    return nil if work_duration.blank?
    return "#{work_duration} Months" if work_duration < 12
    "#{work_duration / 12} Years, #{work_duration % 12} Months"
  end

  def self.disqualify_lead_from_export?(object)
    return if object.blank?
    return false if object.position.blank? if object.class.name.eql?('ContactWatchlistContact')
    position = object.position
    return true if position.blank? || position.email_status.blank? || (position.email =~ Contact::VALID_EMAIL_REGEX).blank?
    if check_disqualification_based_on_personal_email?(object)
      return disqualify_lead_from_export_based_on_personal_email?(object)
    end
    return true unless ['Guaranteed', 'Verified'].include?(position.email_status)
    return false if ['Guaranteed', 'Verified'].any?{|status| position.email_status.eql?(status)}
    #Remove this code after all the positions have been marked as invalid
    EMAIL_VALIDATION_REMARKS.each do |remark|
      return true if position.email_validation_remark.to_s.downcase.squish.include?(remark.downcase)
    end
    false
  end

  def self.required_to_refresh_contact(contact, contact_refresh_time, comparison_time)
    return true if contact.li_updated_at.blank?
    if contact_refresh_time.present? && contact_refresh_time >= 0
      ( contact.li_updated_at <= comparison_time - contact_refresh_time.days )
    else
      ( contact.li_updated_at <= comparison_time - Contact::DATA_REFRESH_AFTER )
    end
  end

  def changed_companies_and_titles_flag(contact_refresh_time, comparison_time)
    contact_refresh_time = contact_refresh_time.present? ? contact_refresh_time : DATA_REFRESH_AFTER_DAYS
    title_changed = contact_positions_created_within_time(comparison_time - contact_refresh_time.days, comparison_time).present?
    last_changed_position = contact_positions_created_within_time(comparison_time - contact_refresh_time.days, comparison_time).first
    changed_company = false
    if last_changed_position.present?
      last_previous_position = contact_positions_prior_to_time(comparison_time - contact_refresh_time.days).first
      if last_previous_position.present?
        changed_company = !last_previous_position.company_id.eql?(last_changed_position.company_id)
      else
        changed_company = true
      end
    end
    { changed_company: changed_company, changed_title: title_changed }
  end

  def current_and_past_companies_and_titles
    { current_company_and_title: current_company_and_title, previous_companies_and_titles: previous_companies_and_titles }
  end

  def current_company_and_title
    latest_current_position = self.positions.where(is_current: true).order('start_date DESC').first
    if latest_current_position.present?
      company_name = latest_current_position.company_name || latest_current_position.company.try(:name)
      title = latest_current_position.title
      return { company: company_name, title: title }
    end
  end

  def previous_companies_and_titles
    company_and_titles = []
    self.positions.where('is_current is FALSE OR is_current IS NULL').order('start_date DESC').find_each do |position|
      company_name = position.company_name || position.company.try(:name)
      title = position.title
      company_and_titles << { company: company_name, title: title }
    end
    company_and_titles
  end

  def contact_positions_created_within_time(start_time, end_time)
    self.positions.where('start_date >= ? AND start_date <= ?', start_time, end_time).order('start_date DESC')
  end

  def contact_positions_prior_to_time(start_time)
    self.positions.where('start_date < ?', start_time).order('start_date DESC')
  end

  def self.check_disqualification_based_on_personal_email?(object)
    if ['DiscoveredContact'].include?(object.class.name)
      # Create different menthods for different object types to be called from here.
      return send("check_disqualification_based_on_personal_email_for_#{object.class.name.downcase}?", object)
    end
  end

  def self.disqualify_lead_from_export_based_on_personal_email?(object)
    if ['DiscoveredContact'].include?(object.class.name)
      # Create different menthods for different object types to be called from here.
      return send("disqualify_lead_from_export_based_on_personal_email_for_#{object.class.name.downcase}?", object)
    end
    false
  end

  def self.check_disqualification_based_on_personal_email_for_discoveredcontact?(object)
    object.lead_discovery.export_results_with_personal_emails_only.present? ? true : false
  end

  def self.disqualify_lead_from_export_based_on_personal_email_for_discoveredcontact?(object)
    object_contact = object.contact
    return true if object_contact.blank?
    if object.lead_discovery.export_results_with_personal_emails_only
      return true if object_contact.personal_email.blank? || object_contact.personal_email_status.blank?
      return true unless ['guaranteed', 'verified'].include?(object_contact.personal_email_status.to_s.downcase)
    end
    false
  end

  def self.contact_info_by_accessed_tier(profile_data, user)
    return [] if profile_data.blank?
    data = profile_data.map do |data|
      accessed_tier = user.subscription_position_histories.find_by(position_id: data["position_id"].to_i).try(:accessed_tier)
      next if accessed_tier.blank?
      hsh = {
        "accessed_tier" => accessed_tier,
        "position_id" => data["position_id"],
        "company_id" => data["company_id"],
        "company_name" => data["company_name"],
        "enrichment_level" => data["enrichment_level"]
      }
      TIER_ATTRS[accessed_tier].each { |attr| hsh[attr.to_s] = data[attr.to_s] }
      hsh
    end
    data.compact
  end
  #Get all contacts those phone verified in position and company
  def self.update_phone_verified(contact_ids)
    phone_verified_contact_ids = Contact.distinct(:id)
                              .joins(:positions, :companies)
                              .where("contacts.id IN (?) and (positions.direct_phone_verified = ? or companies.corporate_phone_verified = ?)",contact_ids, true, true).pluck(:id)
    
    return if phone_verified_contact_ids.blank?
    contact_update_query = phone_verified_contact_ids.map do |id|
      "(true,#{id})"
    end
    contact_update_query = contact_update_query.join(',')
    update_query = <<-SQL
      UPDATE contacts SET
        phone_verified = temp_con.phone_verified
      FROM (
        VALUES #{contact_update_query}
      ) AS temp_con(phone_verified,id)
      WHERE contacts.id = temp_con.id
    SQL
    ActiveRecord::Base.connection.exec_query(update_query)
    UpdateContactIndexesWorker.perform_async(contact_ids: phone_verified_contact_ids)
  end
end
