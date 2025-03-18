"""AdGroupAdStream for Google Ads tap."""

from tap_googleads.dynamic_query_stream import DynamicQueryStream


class AdGroupAdStream(DynamicQueryStream):
    """Ad Group Ad stream"""

    def _get_gaql(self):
        return """
        SELECT
          ad_group.id,
          ad_group_ad.ad.added_by_google_ads,
          ad_group_ad.ad.app_ad.descriptions,
          ad_group_ad.ad.app_ad.headlines,
          ad_group_ad.ad.app_ad.html5_media_bundles,
          ad_group_ad.ad.app_ad.images,
          ad_group_ad.ad.app_ad.mandatory_ad_text,
          ad_group_ad.ad.app_ad.youtube_videos,
          ad_group_ad.ad.app_engagement_ad.descriptions,
          ad_group_ad.ad.app_engagement_ad.headlines,
          ad_group_ad.ad.app_engagement_ad.images,
          ad_group_ad.ad.app_engagement_ad.videos,
          ad_group_ad.ad.call_ad.business_name,
          ad_group_ad.ad.call_ad.call_tracked,
          ad_group_ad.ad.call_ad.conversion_action,
          ad_group_ad.ad.call_ad.conversion_reporting_state,
          ad_group_ad.ad.call_ad.country_code,
          ad_group_ad.ad.call_ad.description1,
          ad_group_ad.ad.call_ad.description2,
          ad_group_ad.ad.call_ad.disable_call_conversion,
          ad_group_ad.ad.call_ad.headline1,
          ad_group_ad.ad.call_ad.headline2,
          ad_group_ad.ad.call_ad.path1,
          ad_group_ad.ad.call_ad.path2,
          ad_group_ad.ad.call_ad.phone_number,
          ad_group_ad.ad.call_ad.phone_number_verification_url,
          ad_group_ad.ad.device_preference,
          ad_group_ad.ad.display_upload_ad.display_upload_product_type,
          ad_group_ad.ad.display_upload_ad.media_bundle,
          ad_group_ad.ad.display_url,
          ad_group_ad.ad.expanded_dynamic_search_ad.description,
          ad_group_ad.ad.expanded_dynamic_search_ad.description2,
          ad_group_ad.ad.expanded_text_ad.description,
          ad_group_ad.ad.expanded_text_ad.description2,
          ad_group_ad.ad.expanded_text_ad.headline_part1,
          ad_group_ad.ad.expanded_text_ad.headline_part2,
          ad_group_ad.ad.expanded_text_ad.headline_part3,
          ad_group_ad.ad.expanded_text_ad.path1,
          ad_group_ad.ad.expanded_text_ad.path2,
          ad_group_ad.ad.final_app_urls,
          ad_group_ad.ad.final_mobile_urls,
          ad_group_ad.ad.final_url_suffix,
          ad_group_ad.ad.final_urls,
          ad_group_ad.ad.hotel_ad,
          ad_group_ad.ad.id,
          ad_group_ad.ad.image_ad.image_url,
          ad_group_ad.ad.image_ad.mime_type,
          ad_group_ad.ad.image_ad.name,
          ad_group_ad.ad.image_ad.pixel_height,
          ad_group_ad.ad.image_ad.pixel_width,
          ad_group_ad.ad.image_ad.preview_image_url,
          ad_group_ad.ad.image_ad.preview_pixel_height,
          ad_group_ad.ad.image_ad.preview_pixel_width,
          ad_group_ad.ad.legacy_app_install_ad,
          ad_group_ad.ad.legacy_responsive_display_ad.accent_color,
          ad_group_ad.ad.legacy_responsive_display_ad.allow_flexible_color,
          ad_group_ad.ad.legacy_responsive_display_ad.business_name,
          ad_group_ad.ad.legacy_responsive_display_ad.call_to_action_text,
          ad_group_ad.ad.legacy_responsive_display_ad.description,
          ad_group_ad.ad.legacy_responsive_display_ad.format_setting,
          ad_group_ad.ad.legacy_responsive_display_ad.logo_image,
          ad_group_ad.ad.legacy_responsive_display_ad.long_headline,
          ad_group_ad.ad.legacy_responsive_display_ad.main_color,
          ad_group_ad.ad.legacy_responsive_display_ad.marketing_image,
          ad_group_ad.ad.legacy_responsive_display_ad.price_prefix,
          ad_group_ad.ad.legacy_responsive_display_ad.promo_text,
          ad_group_ad.ad.legacy_responsive_display_ad.short_headline,
          ad_group_ad.ad.legacy_responsive_display_ad.square_logo_image,
          ad_group_ad.ad.legacy_responsive_display_ad.square_marketing_image,
          ad_group_ad.ad.local_ad.call_to_actions,
          ad_group_ad.ad.local_ad.descriptions,
          ad_group_ad.ad.local_ad.headlines,
          ad_group_ad.ad.local_ad.logo_images,
          ad_group_ad.ad.local_ad.marketing_images,
          ad_group_ad.ad.local_ad.path1,
          ad_group_ad.ad.local_ad.path2,
          ad_group_ad.ad.local_ad.videos,
          ad_group_ad.ad.name,
          ad_group_ad.ad.resource_name,
          ad_group_ad.ad.responsive_display_ad.accent_color,
          ad_group_ad.ad.responsive_display_ad.allow_flexible_color,
          ad_group_ad.ad.responsive_display_ad.business_name,
          ad_group_ad.ad.responsive_display_ad.call_to_action_text,
          ad_group_ad.ad.responsive_display_ad.control_spec.enable_asset_enhancements,
          ad_group_ad.ad.responsive_display_ad.control_spec.enable_autogen_video,
          ad_group_ad.ad.responsive_display_ad.descriptions,
          ad_group_ad.ad.responsive_display_ad.format_setting,
          ad_group_ad.ad.responsive_display_ad.headlines,
          ad_group_ad.ad.responsive_display_ad.logo_images,
          ad_group_ad.ad.responsive_display_ad.long_headline,
          ad_group_ad.ad.responsive_display_ad.main_color,
          ad_group_ad.ad.responsive_display_ad.marketing_images,
          ad_group_ad.ad.responsive_display_ad.price_prefix,
          ad_group_ad.ad.responsive_display_ad.promo_text,
          ad_group_ad.ad.responsive_display_ad.square_logo_images,
          ad_group_ad.ad.responsive_display_ad.square_marketing_images,
          ad_group_ad.ad.responsive_display_ad.youtube_videos,
          ad_group_ad.ad.responsive_search_ad.descriptions,
          ad_group_ad.ad.responsive_search_ad.headlines,
          ad_group_ad.ad.responsive_search_ad.path1,
          ad_group_ad.ad.responsive_search_ad.path2,
          ad_group_ad.ad.shopping_comparison_listing_ad.headline,
          ad_group_ad.ad.shopping_product_ad,
          ad_group_ad.ad.shopping_smart_ad,
          ad_group_ad.ad.smart_campaign_ad.descriptions,
          ad_group_ad.ad.smart_campaign_ad.headlines,
          ad_group_ad.ad.system_managed_resource_source,
          ad_group_ad.ad.text_ad.description1,
          ad_group_ad.ad.text_ad.description2,
          ad_group_ad.ad.text_ad.headline,
          ad_group_ad.ad.tracking_url_template,
          ad_group_ad.ad.type,
          ad_group_ad.ad.url_collections,
          ad_group_ad.ad.url_custom_parameters,
          ad_group_ad.ad.video_ad.in_feed.description1,
          ad_group_ad.ad.video_ad.in_feed.description2,
          ad_group_ad.ad.video_ad.in_feed.headline,
          ad_group_ad.ad.video_ad.in_stream.action_button_label,
          ad_group_ad.ad.video_ad.in_stream.action_headline,
          ad_group_ad.ad.video_ad.out_stream.description,
          ad_group_ad.ad.video_ad.out_stream.headline,
          ad_group_ad.ad.video_responsive_ad.call_to_actions,
          ad_group_ad.ad.video_responsive_ad.companion_banners,
          ad_group_ad.ad.video_responsive_ad.descriptions,
          ad_group_ad.ad.video_responsive_ad.headlines,
          ad_group_ad.ad.video_responsive_ad.long_headlines,
          ad_group_ad.ad.video_responsive_ad.videos,
          ad_group_ad.ad_group,
          ad_group_ad.ad_strength,
          ad_group_ad.labels,
          ad_group_ad.policy_summary.approval_status,
          ad_group_ad.policy_summary.policy_topic_entries,
          ad_group_ad.policy_summary.review_status,
          ad_group_ad.resource_name,
          ad_group_ad.status,
          segments.date
        FROM ad_group_ad
        """

    name = "ad_group_ad"
    primary_keys = ["adGroup__id", "adGroupAd__ad__id"]
    replication_key = "segments__date"
    replication_method = "INCREMENTAL"
    add_date_filter_to_query = True