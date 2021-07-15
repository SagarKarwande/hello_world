class Company < ActiveRecord::Base
    extend CompanyExtension
    extend ElasticsearchExtension
    include CompanyWrapper
    include LocationWrapper
    include Linkedin::ImageOnS3
    include Linkedin::NormalizeName
    include CompanyElasticsearch
  
    enum data_sources: %I[linkedin crunchbase]
  
    DATA_REFRESH_AFTER = 90.days
    RECENT_NEWS_PAGE_NO = 0
    RECENT_NEWS_SIZE = 3
    POPULARITY_LIMIT = 100001
    DEFAULT_IMAGE = "default_company_image.png"
    # Note last value in ranges is 1001, its absolute number anything > 1000 is 
    # low = 1000 and high = nil 
    STANDARD_REVENUE_RANGES = [ 0..1, 1..10, 10..50, 50..100, 100..200, 200..1000 ]
    # Note last value in ranges is 10001, its absolute number anything > 10000 is 
    # low = 10001 and high = nil
    STANDARD_EMPLOYEE_RANGES = [ 1..10, 11..50, 51..200, 201..500, 501..1000, 1001..5000, 5001..10000 ]
  
    validates :md5_linkedin_url, :li_company_id, uniqueness: true, allow_blank: true, on: :create
    #validates :li_company_id, uniqueness: true, allow_blank: true
  
    has_one :company_extended_info, dependent: :destroy
  
    has_many :company_categories, dependent: :destroy
    has_many :categories, through: :company_categories
  
    has_many :company_technologies, dependent: :destroy
    has_many :technologies, through: :company_technologies
  
    has_many :company_rankings, dependent: :destroy
    has_many :rankings , through: :company_rankings
  
    has_many :company_sics, dependent: :destroy
    has_many :sic_descriptions, through: :company_sics
  
    has_many :company_news, dependent: :destroy
  
    has_many :contacts, through: :positions
    has_many :positions
  
    has_many :matched_attendees
    has_many :meetings, through: :matched_attendees
  
    has_many :domains, dependent: :destroy
    has_many :affiliated_companies, dependent: :destroy
  
    has_many :company_watchlist_company, dependent: :destroy
    has_many :company_watchlists, through: :company_watchlist_company
  
    has_many :orb_names, dependent: :destroy
    has_many :orb_domains, dependent: :destroy
  
    has_many :company_naics_codes, dependent: :destroy
    has_many :naics_codes, through: :company_naics_codes
  
    has_many :leads_for_enrichment, dependent: :destroy
    has_many :lead_enrichments, through: :leads_for_enrichment
  
    has_many :cls_audits, dependent: :destroy
    has_many :oxy_audits, dependent: :destroy, class_name: OxyCompanyAudit
  
    has_many :company_locations, dependent: :destroy
  
    has_many :cb_founders, class_name: Founder
    has_many :cb_team_members, class_name: TeamMember
    has_many :cb_board_members, class_name: BoardMember
    has_many :cb_advisor_members, class_name: Advisor
    has_many :cb_cxo_members, class_name: CxoMember
  
    serialize :seo_keywords, Array
    serialize :alexa_data, Hash
  
    after_save do
      save_company_domains
      save_company_type_and_status
      #TODO: Update indexes of all positions when company name is updated
      #self.positions.each{|position| position.__elasticsearch__.index_document}
    end
  
    before_save do 
      map_location
      update_image
      assign_employee_high_and_employee_low if employees_linkedin_changed?
    end
  
    before_validation do
      save_sanitized_li_url_data
      save_linkedin_name
      assign_normalized_name
    end
  
    mount_uploader :ds_logo, AvatarUploader
  
    scope :popular, -> { where("popularity < ?", POPULARITY_LIMIT) }
    scope :non_subsidiary, -> { where("subsidiary is not true" ) }
    default_scope {where(is_to_be_deleted: nil).where.not(linkedin_url: nil).where('(companies.test_data IS NULL OR companies.test_data IS FALSE)') }
  
    def save_company_type_and_status
      CompanyType.find_or_create_by(name: self.company_type_normalized)
      c_status = CompanyStatus.where("lower(name) = (?)", self.status.try(:downcase)).first
      CompanyStatus.create(name: self.status) unless c_status
    end
  
    def profile user
      # Note: we reject null fields from company profile JSON
      ex_info = self.company_extended_info.ex_info if self.company_extended_info
      res = company_json.merge(ex_info || {} ).reject{|k, v| v.blank? }
      res.merge(
        {
          is_watchlisted: is_watchlisted?(user),
          currently_running_process: get_running_enrichment_process_data(currently_running_process: running_enrichment_process),
          li_updated_at: self.li_updated_at,
          is_fresh: !Company.required_to_refresh_company(self, nil, Time.now)
        }
      )
    end
  
    def get_running_enrichment_process_data(currently_running_process: nil)
      currently_running_process = running_enrichment_process if currently_running_process.blank?
      return nil if currently_running_process.blank?
      progress_statuses = AccountProfileRefresh::PROGRESS.map do |progress|
        statuses = progress[:value].map do |value|
          status_for_a_requested_attribute(currently_running_process, value)
        end
        { name: progress[:name], status: overall_status(statuses) }
      end
      { progress_statuses: progress_statuses, status: overall_status(progress_statuses.map{ |status| status[:status] }), started_time: currently_running_process.created_at }
    end
  
    def status_for_a_requested_attribute(currently_running_process, value)
      attribute = currently_running_process.requested_attributes.find{ |attr| attr[:name] == value || attr['name'] == value }
      if attribute.blank?
        AccountProfileRefresh::REQUESTED_ATTRIBUTES_STATUSES[:not_required]
      else
        if currently_running_process.account_profile_refresh_request.present? && currently_running_process.account_profile_refresh_request.bad_data?
          return AccountProfileRefresh::REQUESTED_ATTRIBUTES_STATUSES[:wrong] if attribute[:status].eql?(AccountProfileRefresh::REQUESTED_ATTRIBUTES_STATUSES[:not_started])
        end
        attribute[:status]
      end
    end
  
    def overall_status(statuses)
      if statuses.all? { |str| str.eql?(:not_required) }
        :not_required
      elsif statuses.all? { |str| str.eql?(:completed) }
        :completed
      elsif statuses.any? { |str| str.eql?(:completed) }
        if statuses.any? { |str| str.eql?(:in_progress) } || statuses.any? { |str| str.eql?(:not_started) }
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
  
    def running_enrichment_process
      processed_search_state = AccountProfileRefresh.search_states[:processed]
      AccountProfileRefresh.where(company_id: self.id).where.not(search_state: processed_search_state).order('id').last
    end
  
    def base_info(user = nil, is_current: true, contacts_count: false)
      company_info = self.as_json({ only: [ :id, :name, :industry, :linkedin_url,
                                            :homepage_url, :linkedin_name, :employees_linkedin, 
                                            :li_updated_at, :employee_low, :date_founded,
                                            :ticker, :revenue, :revenue_low, :revenue_high,
                                            :annual_cost_of_revenue, :annual_net_income,
                                            :annual_operating_income, :annual_gross_profit, 
                                            :fiscal_year],
                                    methods: [ :logo_url, :location_area, :employee_range,
                                            :city_name, :country_name, :state_name, :phone,
                                            :street_address, :company_location, :sic_explanations, :sic_codes,
                                            :es_web_traffic_data, :es_seo_keywords, :es_web_technographics] })
      company_info.merge!(is_watchlisted: is_watchlisted?(user)) if user.present?
      company_info.merge!(contacts_count: contacts_count(is_current)) if contacts_count.eql? true
      return company_info
    end
  
    def secondary_industries
      self.cb_industries
    end
  
    def city_name
      city
    end
  
    def country_name
      country_obj = Country.find_by(id: self.country_id)
      country_obj.try(:name) || country
    end
  
    def state_name
      state
    end
  
    def search_info(user = nil, is_current: true)
      return base_info(user, is_current: is_current, contacts_count: true)
    end
  
    def contacts_count(is_current = true)
      select_query = 'count(DISTINCT contact_id) AS number_of_contacts'
      return Position.where(company_id: self.id).select(select_query).as_json.first['number_of_contacts'] if is_current.nil?
      Position.where(company_id: self.id, is_current: is_current).select(select_query).as_json.first['number_of_contacts']
    end
  
    def contacts_count_
      contacts_count(nil)
    end
  
    def contacts_count_true
      contacts_count(true)
    end
  
    def contacts_count_false
      contacts_count(false)
    end
  
    def company_json
      self.as_json({
        only: [ 
          :id, :name, :company_type, :ticker, :industry, :sub_industry, :employees, :employees_linkedin,
          :employee_low, :employee_high, :revenue, :revenue_low, :revenue_high, :date_founded, 
          :fortune_ranking, :subsidiary, :naics, :financial_year_end, :homepage_url, :linkedin_url, 
          :email, :phone, :fax, :description, :updated_at, :annual_cost_of_revenue,
          :annual_gross_profit, :annual_net_income, :annual_operating_income, :fiscal_year, :founders, :web_technologies_enriched_at
        ],
        methods: [
          :headquarters, :actions, :logo_url, :location_area, :sic_codes, :sic_explanations, :company_location, :secondary_industries,
          :specialties, :web_technologies, :es_web_traffic_data, :es_seo_keywords, :es_web_technographics
        ]
      })
    end
  
    def web_technologies
      bw_domain_path = BwDomainPath.where.not(domain: nil).where(company_id: self.id, sub_domain: [nil, ""], url: [nil, ""]).first
      return [] if bw_domain_path.blank?
      bw_domain_path.web_technologies.order(last_detected_at: :desc).pluck('name').uniq
    end
  
    def sic_codes
      sic_descriptions.pluck(:sic_code).join(', ')
    end
  
    def sic_explanations
      sic_descriptions.pluck(:description).join(', ')
    end
  
    def watchlisted_users
      self.company_watchlists.pluck('distinct user_id')
    end
  
    def headquarters 
      self.as_json({
        only: [ :street_address, :city, :state, :country, :zip_code ]
      }).reject{|k, v| v.nil? }
    end
  
    def company_location
      self.headquarters.values.join(', ')
    end
  
    def specialties
      self.categories.collect(&:name).join(",")
    end
  
    def linkedin_followers
      self.company_extended_info.try(:linkedin_followers)
    end
  
    def domain_names
      self.domains.collect(&:domain).join(",")
    end
  
    def country_iso_code
      country_obj = Country.find_by(id: self.country_id)
      country_obj.try(:iso3) || country_obj.try(:iso2) || country_obj.try(:li_country_code)
    end
  
    def news(page_no, size)
      CompanyNews.news_articles(self.id, page_no, size)
    end
  
    def actions
      ["Call", "Email", "Tweet", "Add to Watchlist"]
    end
  
    def get_domain
      if self.homepage_url.present? and self.homepage_url.size > 5
        ::Extensions::Uri.domain_of(homepage_url)
      end
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
      self.meetings.send(scope_name).where("start_time #{operator} ? and user_id = ?", DateTime.now.utc, user_id).limit(3)
    end
  
    def collect_data_from_iv_and_linkedin(is_sleep: true)
      data_from_linkedin(is_sleep: is_sleep)
      #data_from_iv
    end
  
    def data_from_iv
      return true
  =begin
      if self.from_iv.nil? and [false, nil].include?(self.from_linkedin)
        iv = Fetch::InsideViewCompany.new(self)
        iv.save_company_data
      end
  =end
    end
  
    def save_company_from_chrome(data)
      li_company = Fetch::LinkedinCompany.new(self)
      li_company.send(:company_details, data)
      linkedin = MultiSource::Linkedin.new(self, data: li_company.data)
      linkedin.create_or_update_company
    end
  
    def data_from_linkedin(is_sleep: true)
      return unless refresh_linkedin_data?
      sleep rand(SLEEP_RANGE).seconds if is_sleep
      linkedin = MultiSource::Linkedin.new(self)
      linkedin.create_or_update_company
    end
  
    def executives(user:)
      contacts = Contact.where(id: Position.select(:contact_id).where(
        id: Position.select(:id).current.top_execs.where(company_id: self.id) 
      ).limit(10).uniq
                              )
      contacts.map{ |contact| contact.select_fields(user)  }
    end
  
    def c_level_contacts
      inclusion_keywords = ["founder", "co-founder", "chief", "ceo", "coo", "cto", "cmo", "cfo", "cro", "cso", "cpo", "cio", "cdo", "ciso", "clo"]
      exclusion_keywords = ["partner", "assistant", "associate", "administrative", "admin", "office of", "deputy", "contractor", "lead", "of", "to", "vice", "president"]
  
      criteria = {
        "company_ids"=>[self.id],
        "titles" => inclusion_keywords,
        "title_exclusions" => exclusion_keywords,
        "is_current" => true
        }
      fields_to_fetch = [:id, :first_name, :last_name, :popularity, :company_name, :phone, :linkedin_url, :image_url,
      :location, :updated_at, :remark, :industry, :li_updated_at, :country_id, :state_id, :city_id, :email,
      :email_status, :middle_name, :name, :email_status_order, :headline, :logo_url, :location_area, :valid_email,
      :company, :search_data, :search_on_name, :phone_verified]
      result = Contact.es_advance_search_results(criteria, start_index: 0, size: 100, user_id: User.last, select_fields: fields_to_fetch)
      result[:contacts]
    end
  
    def company_founders(user:)
      cb_profile_ids = self.cb_founders.pluck(:cb_person_id)
      contact_ids = CbPerson.where(id: cb_profile_ids).where.not(contact_id: nil).pluck(:contact_id)
      contacts = Contact.where(id: contact_ids)
  
      contacts.map{ |contact| contact.select_fields(user) }
    end
  
    def board_members(user:)
      cb_profile_ids = self.cb_board_members.pluck(:cb_person_id)
      board_members = CbPerson.where(id: cb_profile_ids)
      board_members_json = board_members.map do |board_member|
        if board_member.contact_id.present?
          contact = Contact.find(board_member.contact_id)
          position = contact.positions.where(company_id: self.id).first
          contact.select_fields(user, position: position)
        else
          {
            id: nil,
            first_name: board_member.first_name,
            last_name: board_member.last_name,
            title: "",
            linkedin_url: board_member.linkedin_url,
            email: nil,
            phone: nil,
            headline: "Board Member at #{self.name}"
          }
        end
      end
      board_members_json
    end
  
    def is_watchlisted? user
      user.companies.find_by(id: self.id).present?
    end
  
    def employee_range
      if self.employee_low && self.employee_high
        return "#{self.employee_low} to #{self.employee_high}"
      elsif self.employee_low && self.employee_high.nil?
        "#{self.employee_low}+"
      elsif self.employee_high && self.employee_low.nil?
        "< #{self.employee_high}"
      end
    end
  
    def refresh_linkedin_data?
      return true if self.li_updated_at.blank?
      self.li_updated_at <= DateTime.now - 30.days
    end 
  
    def self.default_industries(mapped = false)
      #industries = Company.where.not(industry: nil).group(:industry).having("count(*) >?", 100).select(:industry).
      #  order(:industry).collect(&:industry).reject(&:empty?)
      #industries = industries.compact.collect(&:titleize).sort
      #return industries
      
      # Do not return default industries from companies. Instead us TpIndustries
      # ::V0::CompanyService.default_industries
      if mapped
        ids = IndustryMapping.pluck('li_industry_id').uniq
        TpIndustry.where(id: ids).order('name').pluck('name')
      else
        TpIndustry.order('name').pluck('name')
      end
    end
  
    def curate_domain
      curation = Enrichment::CurateCompanyDomains.new(self)
      curation.add_priorities
    end
  
    def saved_contacts(page_no, size, user)
      fields = "positions.is_current = true and positions.company_id = ?"
      order = "contacts.first_name, contacts.last_name"
      total_count = user.contacts.joins(:positions).
        where(fields, id).distinct("contacts.id").count
  
      contacts = user.contacts.joins(:positions).
        where(fields, id).distinct("contacts.id").
        order(order).
        limit(size).offset(page_no * size)
  
      return {contacts: contacts, total_count: total_count}
    end
  
  
    def ce_linkedin_res(current_user, append_watchlist_ids: false)
      c_name = linkedin_name || name
      data = {
        id: id,
        li_updated_at: li_updated_at,
        is_watchlisted: is_watchlisted?(current_user),
        linkedin_url: linkedin_url,
        name: name,
        logo_url: logo_url,
        industry: industry,
        employee_low: employee_low,
        employee_high: employee_high,
        country: country.try(:name), 
        from_linkedin: from_linkedin,
        li_member_id: self.li_company_id
      }
      if append_watchlist_ids
        company_watchlist_ids = current_user.company_watchlists.pluck(:id)
        watchlist_ids = CompanyWatchlistCompany.where(company_watchlist_id: company_watchlist_ids, company_id: self.id).pluck(:company_watchlist_id)
        data.merge!({company_watchlist_ids: watchlist_ids})
      end
      return data
    end
  
    def assign_normalized_name
      n_name = normalized_string(self.name)
      self.normalized_name = n_name unless self.normalized_name.eql?(n_name)
      return true
    end
  
    def assign_employee_high_and_employee_low
      return true if employees_linkedin.blank?
  
      if employees_linkedin == 0
        self.employee_low = nil
        self.employee_high = nil
        return true
      end
  
      if employees_linkedin > 10000
        self.employee_low = 10001
        self.employee_high = nil
        return true
      end
  
      Company::STANDARD_EMPLOYEE_RANGES.each do |range|
        if range.include?(employees_linkedin)
          self.employee_low = range.first
          self.employee_high = range.last
          return true
        end
      end
    end
  
    def delete_es_index_document
      query = { query: { term: { id: self.id } } }
      index_present = ( Company.search(query).results.total >= 1 )
      self.__elasticsearch__.delete_document if index_present
    end
  
    def self.company_data(company_ids, user)
      companies = Company.where(id: company_ids).collect{ |c| c.search_info(user) }
      companies = companies.sort_by {|k| k["name"]}
      return companies
    end
  
    def export_crunchbase_data
      founder_ids = self.cb_founders.pluck(:cb_person_id)
      founders = CbPerson.where(id: founder_ids).map {|c| c.name}.join(", ")
      board_members_ids = self.cb_board_members.pluck(:cb_person_id)
      board_members = CbPerson.where(id: board_members_ids).map {|c| c.name}.join(", ")
      cxo_members = self.c_level_contacts.map {|c| c["name"]}.join(", ")
      
      data = [
        self.cb_industries,
        founders,
        board_members,
        cxo_members,
        self.phone,
        self.email
      ]
      extended_info_data = []
      if self.company_extended_info.present?
        ext_info = self.company_extended_info
        extended_info_data = [
          ext_info.twitter_url,
          ext_info.facebook_url,
          ext_info.crunchbase_url,
          ext_info.angellist_url,
          ext_info.youtube_url,
          ext_info.googleplay_url,
          ext_info.itune_url
        ]
      else
        extended_info_data = 7.times.map{|i| ""}
      end
      data.concat(extended_info_data)
    end
  
    def export_funding_data
      if self.company_extended_info.present?
        ext_info = self.company_extended_info
        [
          ext_info.human_readable_total_funding,
          ext_info.human_readable_last_funding,
          ext_info.human_readable_last_funding_date,
          ext_info.last_funding_round_name,
          ext_info.last_funding_lead_investor,
          ext_info.last_funding_total_investors
        ]
      else
        extended_info_data = 6.times.map{|i| ""}
      end
    end
  
    def self.required_to_refresh_company(company, company_refresh_time, comparison_time)
      return true if company.li_updated_at.blank?
      if company_refresh_time.present? && company_refresh_time >= 0
        ( company.li_updated_at <= comparison_time - company_refresh_time.days )
      else
        ( company.li_updated_at <= comparison_time - Company::DATA_REFRESH_AFTER )
      end
    end
  end
  